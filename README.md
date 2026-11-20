# RaftKV

A distributed key-value store with Raft consensus, in zero-dependency Node.js.

Three (or five) nodes elect a leader, replicate a command log, and apply
committed entries to an identical key-value store on every node. A crashed
leader is replaced by a new election; a restarted node rejoins from its
durable log on disk.

> Work in progress — the Raft core is being built in stages; see the commit
> history for the build story.

## Quick start

```bash
# Terminal 1 — node n1
node bin/raftkv.js serve --id n1 --port 8001 \
  --peers n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003 \
  --data ./data-n1

# Terminal 2 — node n2 (and n3 on 8003 the same way)
node bin/raftkv.js serve --id n2 --port 8002 \
  --peers n1=http://127.0.0.1:8001,n3=http://127.0.0.1:8003 \
  --data ./data-n2
```

Then, against any node (writes are forwarded to the leader):

```bash
node bin/raftkv.js put greeting hello --node http://127.0.0.1:8001
node bin/raftkv.js get greeting --node http://127.0.0.1:8002
node bin/raftkv.js status --node http://127.0.0.1:8003
```

## Tests

```bash
npm test
```

## License

MIT
