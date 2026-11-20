import test from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { RaftNode } from '../src/node.js';
import { PersistentState } from '../src/persistence.js';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(fn, timeout = 5000, interval = 25) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = fn();
    if (value) return value;
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await delay(interval);
  }
}

// A 3-node cluster wired together in-process: `send` delivers the RPC
// straight to the target node's handler after a small random delay, the way
// a real network would.
async function makeCluster(t, ids = ['n1', 'n2', 'n3']) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'raftkv-cluster-'));
  const nodes = new Map();
  for (const id of ids) {
    const state = await new PersistentState(path.join(dir, id)).load();
    const node = new RaftNode({
      id,
      peers: ids.filter((x) => x !== id),
      send: async (peerId, rpc) => {
        const target = nodes.get(peerId);
        if (!target || !target.started) return null;
        await delay(1 + Math.random() * 4);
        if (!target.started) return null;
        return rpc.type === 'request-vote'
          ? target.requestVote(rpc)
          : target.appendEntries(rpc);
      },
      state,
      electionTimeout: [60, 120],
      heartbeatMs: 20,
    });
    nodes.set(id, node);
  }
  for (const node of nodes.values()) node.start();
  t.after(async () => {
    for (const node of nodes.values()) node.stop();
    await fs.rm(dir, { recursive: true, force: true });
  });
  return { nodes, dir };
}

function leaders(nodes) {
  return [...nodes.values()].filter((n) => n.role === 'leader');
}

test('cluster elects exactly one leader', async (t) => {
  const { nodes } = await makeCluster(t);
  const leader = await waitFor(() => (leaders(nodes).length === 1 ? leaders(nodes)[0] : null));
  assert.ok(leader);
  // Stability: still exactly one leader a second later, in the same term.
  await delay(1000);
  assert.equal(leaders(nodes).length, 1);
  assert.equal(leaders(nodes)[0].id, leader.id);
});

test('writes replicate to every node and become visible', async (t) => {
  const { nodes } = await makeCluster(t);
  const leader = await waitFor(() => leaders(nodes)[0] ?? null);
  const res = await leader.submit({ op: 'put', key: 'greeting', value: 'hello' });
  assert.equal(res.ok, true);
  await waitFor(() =>
    [...nodes.values()].every((n) => n.store.get('greeting') === 'hello') ? true : null,
  );
  for (const node of nodes.values()) {
    assert.equal(node.commitIndex, 1);
    assert.equal(node.lastApplied, 1);
  }
});

test('a follower refuses writes and names the leader', async (t) => {
  const { nodes } = await makeCluster(t);
  const leader = await waitFor(() => leaders(nodes)[0] ?? null);
  const follower = [...nodes.values()].find((n) => n !== leader);
  await waitFor(() => (follower.leaderId === leader.id ? true : null));
  const res = await follower.submit({ op: 'put', key: 'x', value: 1 });
  assert.equal(res.ok, false);
  assert.equal(res.leaderId, leader.id);
});

test('a new leader is elected after the leader stops, and writes continue', async (t) => {
  const { nodes } = await makeCluster(t);
  const oldLeader = await waitFor(() => leaders(nodes)[0] ?? null);
  await oldLeader.submit({ op: 'put', key: 'before', value: 'crash' });
  oldLeader.stop();
  const newLeader = await waitFor(() => {
    const alive = leaders(nodes).filter((n) => n.started && n !== oldLeader);
    return alive.length === 1 ? alive[0] : null;
  });
  assert.notEqual(newLeader.id, oldLeader.id);
  assert.ok(newLeader.currentTerm > oldLeader.currentTerm);
  const res = await newLeader.submit({ op: 'put', key: 'after', value: 'failover' });
  assert.equal(res.ok, true);
  // The surviving nodes kept the pre-crash write.
  assert.equal(newLeader.store.get('before'), 'crash');
  assert.equal(newLeader.store.get('after'), 'failover');
});

test('a restarted node rejoins from its durable log and catches up', async (t) => {
  const { nodes, dir } = await makeCluster(t);
  const leader = await waitFor(() => leaders(nodes)[0] ?? null);
  await leader.submit({ op: 'put', key: 'a', value: 1 });
  await leader.submit({ op: 'put', key: 'b', value: 2 });

  const follower = [...nodes.values()].find((n) => n !== leader);
  // Wait until this follower has actually replicated both entries; commit
  // only needs a majority, so it may lag the leader.
  await waitFor(() => (follower.state.log.lastIndex() === 2 ? true : null));
  follower.stop();
  // Writes continue while the follower is down.
  await leader.submit({ op: 'put', key: 'c', value: 3 });

  // Restart the same node from the same data directory: its log is intact,
  // and the leader back-fills the entries it missed.
  const state = await new PersistentState(path.join(dir, follower.id)).load();
  assert.equal(state.log.lastIndex(), 2, 'durable log survived the restart');
  const restarted = new RaftNode({
    id: follower.id,
    peers: follower.peers,
    send: follower.send,
    state,
    electionTimeout: [60, 120],
    heartbeatMs: 20,
  });
  nodes.set(follower.id, restarted);
  restarted.start();
  await waitFor(() => (restarted.store.get('c') === 3 ? true : null));
  assert.equal(restarted.store.get('a'), 1);
  assert.equal(restarted.store.get('b'), 2);
});

test('RPCs from a stale term are rejected', async (t) => {
  const { nodes } = await makeCluster(t);
  const leader = await waitFor(() => leaders(nodes)[0] ?? null);
  const follower = [...nodes.values()].find((n) => n !== leader);
  await waitFor(() => (follower.leaderId === leader.id ? true : null));
  const staleVote = await follower.requestVote({
    term: 0,
    candidateId: 'nX',
    lastLogIndex: 0,
    lastLogTerm: 0,
  });
  assert.equal(staleVote.voteGranted, false);
  const staleAppend = await follower.appendEntries({
    term: 0,
    leaderId: 'nX',
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
  });
  assert.equal(staleAppend.success, false);
  assert.equal(follower.leaderId, leader.id, 'stale leader did not take over');
});

test('a candidate with a shorter log cannot win an election', async (t) => {
  const { nodes } = await makeCluster(t);
  const leader = await waitFor(() => leaders(nodes)[0] ?? null);
  await leader.submit({ op: 'put', key: 'k', value: 'v' });
  await waitFor(() =>
    [...nodes.values()].every((n) => n.store.get('k') === 'v') ? true : null,
  );
  // A vote request from a candidate whose log is behind must be refused.
  const follower = [...nodes.values()].find((n) => n !== leader);
  const res = await follower.requestVote({
    term: leader.currentTerm + 5,
    candidateId: 'nX',
    lastLogIndex: 0,
    lastLogTerm: 0,
  });
  assert.equal(res.voteGranted, false);
});
