# RaftKV — a distributed key-value store with Raft consensus

A 3-node (or 5-node) key-value store where every write goes through the
[Raft consensus algorithm](https://raft.github.io/): nodes elect a leader,
replicate a command log, and apply committed entries to an identical
key-value store on every node. Kill the leader and a new one is elected in
under a second; writes continue and no committed data is lost.

Zero runtime dependencies — plain Node.js (`node:http`, global `fetch`,
`node:test`).

## Quick start

Start three nodes in three terminals:

```bash
# Terminal 1
node bin/raftkv.js serve --id n1 --port 8001 \
  --peers n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003 \
  --data ./data-n1

# Terminal 2 — same, with --id n2 --port 8002 and its own peer list / data dir
# Terminal 3 — same, with --id n3 --port 8003
```

Then talk to any node — writes are forwarded to the leader automatically:

```bash
node bin/raftkv.js put greeting hello --node http://127.0.0.1:8001
node bin/raftkv.js get greeting       --node http://127.0.0.1:8002
node bin/raftkv.js keys               --node http://127.0.0.1:8003
node bin/raftkv.js delete greeting    --node http://127.0.0.1:8001
node bin/raftkv.js status             --node http://127.0.0.1:8001
```

Kill the leader's terminal and watch a new leader get elected; `put` keeps
working and the data written before the crash is still there.

## How it works

```
            ┌────────────┐  AppendEntries (log + heartbeat)  ┌────────────┐
            │   leader   │ ────────────────────────────────▶ │  follower  │
            │  (n1)      │ ◀── RequestVote / AppendEntries ── │  (n2, n3)  │
            └────────────┘                                   └────────────┘
   client ──▶ PUT /kv/:key ──▶ log entry ──▶ majority replicates
              ──▶ commit ──▶ apply to KV store on every node
```

- **Leader election** — a follower that hears nothing for a randomised
  150–300 ms becomes a candidate, increments its term, votes for itself and
  asks the rest. First candidate to reach a majority wins; the random
  timeouts keep split votes rare and short-lived.
- **Log replication** — the leader streams entries with `AppendEntries`
  RPCs. A follower rejects an RPC whose `prevLogIndex`/`prevLogTerm` does
  not match its own log, and the leader backs up one entry at a time until
  the logs agree — the standard Raft consistency check, so a restarted
  node re-syncs instead of diverging.
- **Commit rule** — an entry commits once a majority holds it, and only
  entries from the leader's current term advance the commit index, which is
  what keeps a stale leader from committing entries from an old term.
- **Durable state** — `currentTerm`, `votedFor` and the log are persisted
  to disk on every change (atomic write via tmp file + rename, serialised
  through a promise chain). A restarted node rejoins with its log intact
  and the leader back-fills whatever it missed.
- **Client API** — `GET /kv/:key`, `PUT /kv/:key`, `DELETE /kv/:key`,
  `GET /kv`, `GET /status`. A follower answers a write with `409` and the
  leader's URL, so the CLI can retry there. Reads are served from the
  node's applied state (fast, possibly stale on a follower).

## Design decisions

1. **Transport-agnostic core.** `RaftNode` takes an injected `send(peerId,
   rpc)` function and knows nothing about HTTP. The tests wire it to
   in-process calls with random delays, so the consensus logic is tested
   without sockets; the HTTP layer is a thin adapter.
2. **Zero dependencies.** Raft's RPCs are plain JSON over `fetch` /
   `node:http`, so the project installs nothing and the algorithm is the
   only thing to read.
3. **Deterministic state machine.** Commands are plain `{ op, key, value }`
   objects applied in log order, so every node that applies the same log
   ends up with the same store — the property consensus exists to provide.
4. **Safety over liveness on the edges.** A stale term is rejected before
   any state changes; a candidate whose log is behind loses the vote
   (the up-to-date check); writes on a follower fail fast with the
   leader's address instead of silently queuing.

## Project layout

```
bin/raftkv.js        CLI: serve / put / get / delete / keys / status
src/node.js          Raft core: elections, voting, replication, commit
src/log.js           1-indexed replicated log
src/persistence.js   durable term / vote / log (atomic JSON on disk)
src/store.js         the KV state machine
src/transport.js     Raft RPCs over HTTP (fetch)
src/server.js        HTTP server: /raft/* RPCs + /kv/* client API
tests/               node:test — unit, in-process cluster, real HTTP cluster
```

## Tests

```bash
npm test        # 23 tests, 6 consecutive green runs locally
```

Coverage: log indexing/truncation, store semantics, persistence round-trip
and concurrent writes, single-leader election and stability, replication to
all nodes, follower write rejection, leader failover with writes
continuing, node restart from its durable log, stale-term rejection,
log-up-to-date voting, and a full HTTP end-to-end test (elect → write →
read from every node → kill leader → fail over → write again).

## Limitations (honest ones)

- No log compaction / snapshotting yet — the log grows forever, so this is
  a learning implementation, not a production store.
- No membership changes — the peer set is fixed at startup (joint
  consensus would be the next feature).
- Reads are not linearizable on followers; only leader reads are
  guaranteed fresh.

## License

MIT
