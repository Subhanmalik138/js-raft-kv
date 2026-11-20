import { KVStore } from './store.js';

export const FOLLOWER = 'follower';
export const CANDIDATE = 'candidate';
export const LEADER = 'leader';

// A single Raft node. The consensus logic is transport-agnostic: RPCs travel
// through the injected `send(peerId, rpc)` function, which the HTTP server
// wires to real HTTP and the tests wire to in-process calls. That keeps the
// algorithm testable without sockets.
//
// RPC shapes (both directions are plain JSON):
//   { type: 'request-vote', term, candidateId, lastLogIndex, lastLogTerm }
//     -> { term, voteGranted }
//   { type: 'append-entries', term, leaderId, prevLogIndex, prevLogTerm,
//        entries: [{ term, command }], leaderCommit }
//     -> { term, success }
export class RaftNode {
  constructor({
    id,
    peers = [],
    send,
    state,
    store = new KVStore(),
    electionTimeout = [150, 300],
    heartbeatMs = 50,
  }) {
    this.id = id;
    this.peers = [...peers];
    this.send = send;
    this.state = state; // PersistentState: currentTerm, votedFor, log
    this.store = store;
    this.electionTimeout = electionTimeout;
    this.heartbeatMs = heartbeatMs;

    this.role = FOLLOWER;
    this.leaderId = null;
    this.commitIndex = 0;
    this.lastApplied = 0;
    this.votesReceived = new Set();
    this.nextIndex = new Map(); // leader only: peerId -> next log index to send
    this.matchIndex = new Map(); // leader only: peerId -> highest replicated index
    this.pending = new Map(); // logIndex -> { resolve, reject } for submit()
    this.electionTimer = null;
    this.heartbeatTimer = null;
    this.started = false;
  }

  get currentTerm() {
    return this.state.currentTerm;
  }

  get clusterSize() {
    return this.peers.length + 1;
  }

  majority() {
    return Math.floor(this.clusterSize / 2) + 1;
  }

  // -- lifecycle -----------------------------------------------------------

  start() {
    if (this.started) return;
    this.started = true;
    this.resetElectionTimer();
  }

  stop() {
    this.started = false;
    clearTimeout(this.electionTimer);
    clearInterval(this.heartbeatTimer);
    this.electionTimer = null;
    this.heartbeatTimer = null;
  }

  resetElectionTimer() {
    if (!this.started) return;
    clearTimeout(this.electionTimer);
    const [lo, hi] = this.electionTimeout;
    const timeout = lo + Math.random() * (hi - lo);
    this.electionTimer = setTimeout(() => this.startElection(), timeout);
  }

  // -- elections -----------------------------------------------------------

  async startElection() {
    if (!this.started || this.role === LEADER) return;
    this.role = CANDIDATE;
    this.state.currentTerm += 1;
    this.state.votedFor = this.id;
    this.leaderId = null;
    await this.state.persist();
    this.votesReceived = new Set([this.id]);
    this.resetElectionTimer();

    const args = {
      type: 'request-vote',
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex: this.state.log.lastIndex(),
      lastLogTerm: this.state.log.lastTerm(),
    };
    const termAtStart = this.currentTerm;
    await Promise.all(
      this.peers.map(async (peerId) => {
        try {
          const res = await this.send(peerId, args);
          if (!res || this.role !== CANDIDATE || this.currentTerm !== termAtStart) return;
          if (res.term > this.currentTerm) {
            await this.stepDown(res.term);
            return;
          }
          if (res.voteGranted) {
            this.votesReceived.add(peerId);
            if (this.votesReceived.size >= this.majority()) {
              this.becomeLeader();
            }
          }
        } catch {
          // Unreachable peer: an election can still succeed with the rest.
        }
      }),
    );
  }

  becomeLeader() {
    if (this.role === LEADER) return;
    this.role = LEADER;
    this.leaderId = this.id;
    clearTimeout(this.electionTimer);
    const lastIndex = this.state.log.lastIndex();
    for (const peerId of this.peers) {
      this.nextIndex.set(peerId, lastIndex + 1);
      this.matchIndex.set(peerId, 0);
    }
    this.sendHeartbeats();
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => this.sendHeartbeats(), this.heartbeatMs);
  }

  async stepDown(term) {
    if (term > this.currentTerm) {
      this.state.currentTerm = term;
      this.state.votedFor = null;
      await this.state.persist();
    }
    const wasLeader = this.role === LEADER;
    this.role = FOLLOWER;
    this.leaderId = null;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    if (wasLeader) this.rejectPending(new Error('lost leadership'));
    this.resetElectionTimer();
  }

  // -- RPC handlers (called by the transport) ------------------------------

  async requestVote(args) {
    if (args.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }
    if (args.term > this.currentTerm) {
      await this.stepDown(args.term);
    }
    const logOk =
      args.lastLogTerm > this.state.log.lastTerm() ||
      (args.lastLogTerm === this.state.log.lastTerm() &&
        args.lastLogIndex >= this.state.log.lastIndex());
    const canVote = this.state.votedFor === null || this.state.votedFor === args.candidateId;
    if (logOk && canVote) {
      this.state.votedFor = args.candidateId;
      await this.state.persist();
      this.resetElectionTimer();
      return { term: this.currentTerm, voteGranted: true };
    }
    return { term: this.currentTerm, voteGranted: false };
  }

  async appendEntries(args) {
    if (args.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }
    if (args.term > this.currentTerm) {
      await this.stepDown(args.term);
    } else if (this.role === CANDIDATE) {
      // A legitimate leader for this term exists: stand down.
      this.role = FOLLOWER;
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.leaderId = args.leaderId;
    this.resetElectionTimer();

    // Log consistency check: our log must contain an entry at prevLogIndex
    // with the same term the leader claims.
    if (args.prevLogIndex > this.state.log.lastIndex()) {
      return { term: this.currentTerm, success: false };
    }
    if (this.state.log.termAt(args.prevLogIndex) !== args.prevLogTerm) {
      return { term: this.currentTerm, success: false };
    }

    // Append new entries, deleting any conflicting tail first.
    let index = args.prevLogIndex;
    let changed = false;
    for (const entry of args.entries ?? []) {
      index += 1;
      const existing = this.state.log.entryAt(index);
      if (existing && existing.term !== entry.term) {
        this.state.log.truncateFrom(index);
        changed = true;
      }
      if (!this.state.log.entryAt(index)) {
        this.state.log.append(entry.term, entry.command);
        changed = true;
      }
    }
    if (changed) await this.state.persist();

    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.state.log.lastIndex());
      this.applyCommitted();
    }
    return { term: this.currentTerm, success: true };
  }

  // -- leader replication ---------------------------------------------------

  sendHeartbeats() {
    if (this.role !== LEADER) return;
    for (const peerId of this.peers) this.replicateTo(peerId);
  }

  async replicateTo(peerId) {
    if (this.role !== LEADER) return;
    const next = this.nextIndex.get(peerId) ?? this.state.log.lastIndex() + 1;
    const prevLogIndex = next - 1;
    const args = {
      type: 'append-entries',
      term: this.currentTerm,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm: this.state.log.termAt(prevLogIndex),
      entries: this.state.log.sliceAfter(prevLogIndex),
      leaderCommit: this.commitIndex,
    };
    let res;
    try {
      res = await this.send(peerId, args);
    } catch {
      return; // peer down: next heartbeat retries
    }
    if (!res || this.role !== LEADER) return;
    if (res.term > this.currentTerm) {
      await this.stepDown(res.term);
      return;
    }
    if (res.success) {
      const sent = args.entries.length;
      this.matchIndex.set(peerId, prevLogIndex + sent);
      this.nextIndex.set(peerId, prevLogIndex + sent + 1);
      this.tryAdvanceCommit();
    } else {
      // Log mismatch: back up one entry and retry on the next heartbeat.
      this.nextIndex.set(peerId, Math.max(1, next - 1));
    }
  }

  tryAdvanceCommit() {
    // Advance commitIndex to the largest N (from our own term) replicated on
    // a majority, then apply and resolve waiting clients.
    for (let n = this.state.log.lastIndex(); n > this.commitIndex; n--) {
      if (this.state.log.termAt(n) !== this.currentTerm) continue;
      let count = 1; // the leader itself
      for (const peerId of this.peers) {
        if ((this.matchIndex.get(peerId) ?? 0) >= n) count += 1;
      }
      if (count >= this.majority()) {
        this.commitIndex = n;
        this.applyCommitted();
        return;
      }
    }
  }

  applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      const entry = this.state.log.entryAt(this.lastApplied);
      if (entry) this.store.apply(entry.command);
      const waiter = this.pending.get(this.lastApplied);
      if (waiter) {
        this.pending.delete(this.lastApplied);
        waiter.resolve({ committed: true, index: this.lastApplied });
      }
    }
  }

  rejectPending(err) {
    for (const waiter of this.pending.values()) waiter.reject(err);
    this.pending.clear();
  }

  // -- client API ------------------------------------------------------------

  // Submit a command to the replicated log. Only the leader accepts writes;
  // followers report who the leader is so clients can retry there.
  async submit(command) {
    if (this.role !== LEADER) {
      return { ok: false, leaderId: this.leaderId };
    }
    const index = this.state.log.append(this.currentTerm, command);
    await this.state.persist();
    const promise = new Promise((resolve, reject) => {
      this.pending.set(index, { resolve, reject });
      setTimeout(() => {
        if (this.pending.has(index)) {
          this.pending.delete(index);
          reject(new Error('commit timed out'));
        }
      }, 5000);
    });
    for (const peerId of this.peers) this.replicateTo(peerId);
    try {
      await promise;
      return { ok: true, index };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  }

  status() {
    return {
      id: this.id,
      role: this.role,
      term: this.currentTerm,
      leaderId: this.leaderId,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      logLength: this.state.log.lastIndex(),
      keys: this.store.size,
      peers: this.peers,
    };
  }
}
