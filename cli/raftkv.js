#!/usr/bin/env node
// raftkv — CLI for a RaftKV cluster.
//
//   raftkv serve --id n1 --port 8001 \
//     --peers n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003 \
//     --data ./data-n1
//   raftkv put greeting hello --node http://127.0.0.1:8001
//   raftkv get greeting       --node http://127.0.0.1:8002
//   raftkv delete greeting    --node http://127.0.0.1:8001
//   raftkv keys               --node http://127.0.0.1:8001
//   raftkv status             --node http://127.0.0.1:8001
import { RaftNode } from '../src/node.js';
import { PersistentState } from '../src/persistence.js';
import { createServer } from '../src/server.js';
import { createHttpSend } from '../src/transport.js';

function parseArgs(argv) {
  const positional = [];
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      flags[key] = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : true;
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function parsePeers(spec) {
  const urls = {};
  if (!spec) return urls;
  for (const pair of String(spec).split(',')) {
    const [id, url] = pair.split('=');
    if (id && url) urls[id] = url.replace(/\/$/, '');
  }
  return urls;
}

async function serve(flags) {
  const id = flags.id;
  const port = Number(flags.port ?? 8001);
  const peerUrls = parsePeers(flags.peers);
  const dataDir = flags.data ?? `./data-${id}`;
  if (!id) {
    console.error('serve requires --id');
    process.exit(1);
  }
  const state = await new PersistentState(dataDir).load();
  const node = new RaftNode({
    id,
    peers: Object.keys(peerUrls),
    send: createHttpSend(peerUrls),
    state,
  });
  const server = createServer(node, peerUrls);
  node.start();
  server.listen(port, () => {
    console.log(`raftkv node ${id} listening on :${port} (peers: ${Object.keys(peerUrls).join(', ') || 'none'})`);
  });
  const shutdown = () => {
    node.stop();
    server.close(() => process.exit(0));
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Client commands: talk to --node, follow a 409 leader redirect once.
async function request(nodeUrl, path, options = {}) {
  const res = await fetch(`${nodeUrl}${path}`, options);
  const body = await res.json().catch(() => ({}));
  return { status: res.status, body };
}

async function withLeader(nodeUrl, fn) {
  let result = await fn(nodeUrl);
  if (result.status === 409 && result.body.leaderUrl) {
    console.error(`not leader — retrying at ${result.body.leaderUrl}`);
    result = await fn(result.body.leaderUrl);
  }
  return result;
}

function fail(result) {
  console.error(JSON.stringify(result.body, null, 2));
  process.exit(1);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const [command, key, value] = positional;
  const nodeUrl = String(flags.node ?? 'http://127.0.0.1:8001').replace(/\/$/, '');

  if (command === 'serve') return serve(flags);

  if (command === 'status') {
    const { body } = await request(nodeUrl, '/status');
    console.log(JSON.stringify(body, null, 2));
    return;
  }
  if (command === 'keys') {
    const { body } = await request(nodeUrl, '/kv');
    console.log(JSON.stringify(body.keys ?? {}, null, 2));
    return;
  }
  if (command === 'get' && key) {
    const { status, body } = await request(nodeUrl, `/kv/${encodeURIComponent(key)}`);
    if (status !== 200) fail({ status, body });
    console.log(JSON.stringify(body.value));
    return;
  }
  if (command === 'put' && key && value !== undefined) {
    const result = await withLeader(nodeUrl, (base) =>
      request(base, `/kv/${encodeURIComponent(key)}`, {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ value }),
      }),
    );
    if (result.status !== 200) fail(result);
    console.log(JSON.stringify({ key, value, committedIndex: result.body.index }));
    return;
  }
  if (command === 'delete' && key) {
    const result = await withLeader(nodeUrl, (base) =>
      request(base, `/kv/${encodeURIComponent(key)}`, { method: 'DELETE' }),
    );
    if (result.status !== 200) fail(result);
    console.log(JSON.stringify({ key, deleted: true, committedIndex: result.body.index }));
    return;
  }

  console.error(`usage:
  raftkv serve --id <id> --port <port> --peers <id=url,...> [--data <dir>]
  raftkv put <key> <value> [--node <url>]
  raftkv get <key> [--node <url>]
  raftkv delete <key> [--node <url>]
  raftkv keys [--node <url>]
  raftkv status [--node <url>]`);
  process.exit(1);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
