// HTTP transport for Raft RPCs. A peer is addressed by its base URL; each
// RPC is a POST of the plain-JSON message the Raft core already defines.
// A failed fetch resolves to null (peer down / slow) instead of throwing,
// so the Raft core treats it as a lost RPC and retries on the next heartbeat.
export function createHttpSend(peerUrls, timeoutMs = 2000) {
  return async function send(peerId, rpc) {
    const base = peerUrls[peerId];
    if (!base) return null;
    const path = rpc.type === 'request-vote' ? '/raft/request-vote' : '/raft/append-entries';
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(`${base}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(rpc),
        signal: controller.signal,
      });
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  };
}
