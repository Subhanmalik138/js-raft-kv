import http from 'node:http';

// HTTP front end for a Raft node. Two groups of routes:
//
//   Raft RPCs   POST /raft/request-vote, POST /raft/append-entries
//   Client API  GET /kv/:key, PUT /kv/:key, DELETE /kv/:key,
//               GET /kv (all keys), GET /status
//
// Writes go through Raft: only the leader appends to the log. A follower
// answers 409 with the leader's URL so the CLI can retry there. Reads are
// served from the node's applied state — fast, but possibly stale on a
// follower; use the leader's /status + GET for a read-after-write guarantee.
export function createServer(node, peerUrls) {
  const server = http.createServer(async (req, res) => {
    try {
      await handle(node, peerUrls, req, res);
    } catch (err) {
      sendJson(res, 500, { error: err.message });
    }
  });
  return server;
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(body);
}

async function readJson(req, limit = 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw new Error('request body too large');
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function leaderUrl(peerUrls, leaderId) {
  return leaderId ? peerUrls[leaderId] ?? null : null;
}

async function handle(node, peerUrls, req, res) {
  const url = new URL(req.url, 'http://localhost');
  const parts = url.pathname.split('/').filter(Boolean);

  if (req.method === 'POST' && url.pathname === '/raft/request-vote') {
    sendJson(res, 200, await node.requestVote(await readJson(req)));
    return;
  }
  if (req.method === 'POST' && url.pathname === '/raft/append-entries') {
    sendJson(res, 200, await node.appendEntries(await readJson(req)));
    return;
  }

  if (req.method === 'GET' && url.pathname === '/status') {
    sendJson(res, 200, node.status());
    return;
  }

  if (req.method === 'GET' && url.pathname === '/kv') {
    sendJson(res, 200, { keys: node.store.snapshot() });
    return;
  }

  if (parts[0] === 'kv' && parts[1]) {
    const key = decodeURIComponent(parts[1]);
    if (req.method === 'GET') {
      const value = node.store.get(key);
      if (value === undefined) return sendJson(res, 404, { error: 'key not found', key });
      return sendJson(res, 200, { key, value });
    }
    if (req.method === 'PUT' || req.method === 'DELETE') {
      const body = req.method === 'PUT' ? await readJson(req) : {};
      const command =
        req.method === 'PUT'
          ? { op: 'put', key, value: body.value ?? null }
          : { op: 'delete', key };
      const result = await node.submit(command);
      if (result.ok) return sendJson(res, 200, { ...result, key });
      return sendJson(res, 409, {
        error: 'not the leader',
        leaderId: result.leaderId ?? null,
        leaderUrl: leaderUrl(peerUrls, result.leaderId),
        detail: result.error ?? null,
      });
    }
  }

  sendJson(res, 404, { error: 'not found' });
}
