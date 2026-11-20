import test from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { RaftNode } from '../src/node.js';
import { PersistentState } from '../src/persistence.js';
import { createServer } from '../src/server.js';
import { createHttpSend } from '../src/transport.js';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(fn, timeout = 8000, interval = 50) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = await fn();
    if (value) return value;
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await delay(interval);
  }
}

async function getJson(url) {
  const res = await fetch(url);
  return { status: res.status, body: await res.json() };
}

// Full end-to-end test over real HTTP: three processes' worth of nodes in
// one test process, on real sockets, exercised through the public API.
test('HTTP cluster: elect, write, read, fail over, write again', async (t) => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), 'raftkv-http-'));
  const ports = { n1: 18301, n2: 18302, n3: 18303 };
  const urls = Object.fromEntries(
    Object.entries(ports).map(([id, port]) => [id, `http://127.0.0.1:${port}`]),
  );
  const servers = [];
  const nodes = new Map();

  for (const [id, port] of Object.entries(ports)) {
    const peerUrls = Object.fromEntries(
      Object.entries(urls).filter(([peerId]) => peerId !== id),
    );
    const state = await new PersistentState(path.join(baseDir, id)).load();
    const node = new RaftNode({
      id,
      peers: Object.keys(peerUrls),
      send: createHttpSend(peerUrls),
      state,
      electionTimeout: [150, 300],
      heartbeatMs: 50,
    });
    const server = createServer(node, peerUrls);
    await new Promise((resolve) => server.listen(port, resolve));
    node.start();
    servers.push(server);
    nodes.set(id, node);
  }
  t.after(async () => {
    for (const node of nodes.values()) node.stop();
    for (const server of servers) await new Promise((resolve) => server.close(resolve));
    await fs.rm(baseDir, { recursive: true, force: true });
  });

  // Exactly one leader emerges.
  const leaderId = await waitFor(async () => {
    const statuses = await Promise.all(
      Object.values(urls).map((u) => getJson(`${u}/status`)),
    );
    const leaders = statuses.filter((s) => s.body.role === 'leader');
    return leaders.length === 1 ? leaders[0].body.id : null;
  });
  const leaderUrl = urls[leaderId];

  // Write through a follower: the API answers 409 with the leader URL.
  const followerUrl = Object.values(urls).find((u) => u !== leaderUrl);
  const putRes = await fetch(`${followerUrl}/kv/city`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'Lahore' }),
  });
  assert.equal(putRes.status, 409);
  const redirect = await putRes.json();
  assert.equal(redirect.leaderUrl, leaderUrl);

  // Write through the leader, read from every node.
  const okPut = await fetch(`${leaderUrl}/kv/city`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'Lahore' }),
  });
  assert.equal(okPut.status, 200);
  await waitFor(async () => {
    const reads = await Promise.all(Object.values(urls).map((u) => getJson(`${u}/kv/city`)));
    return reads.every((r) => r.body.value === 'Lahore') ? true : null;
  });

  // Kill the leader process (server + node) and confirm failover.
  const leaderNode = nodes.get(leaderId);
  const leaderServer = servers[Object.keys(ports).indexOf(leaderId)];
  leaderNode.stop();
  await new Promise((resolve) => leaderServer.close(resolve));
  const newLeaderId = await waitFor(async () => {
    const alive = Object.entries(urls).filter(([id]) => id !== leaderId);
    const statuses = await Promise.all(
      alive.map(([id, u]) => getJson(`${u}/status`).catch(() => null)),
    );
    const leaders = statuses.filter((s) => s && s.body.role === 'leader');
    return leaders.length === 1 ? leaders[0].body.id : null;
  });
  assert.notEqual(newLeaderId, leaderId);

  const put2 = await fetch(`${urls[newLeaderId]}/kv/after`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'failover' }),
  });
  assert.equal(put2.status, 200);
  const read2 = await getJson(`${urls[newLeaderId]}/kv/after`);
  assert.equal(read2.body.value, 'failover');
  const readOld = await getJson(`${urls[newLeaderId]}/kv/city`);
  assert.equal(readOld.body.value, 'Lahore', 'pre-crash data survived failover');
});
