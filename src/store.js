// The state machine Raft replicates: a plain key-value store. Every command
// is deterministic, so every node that applies the same log in the same order
// ends up with the same store — that is the whole point of consensus.
//
// Commands:
//   { op: 'put', key, value }     — value must be JSON-serialisable
//   { op: 'delete', key }
export class KVStore {
  constructor() {
    this.map = new Map();
  }

  apply(command) {
    if (!command || typeof command !== 'object') return null;
    if (command.op === 'put' && typeof command.key === 'string') {
      this.map.set(command.key, command.value ?? null);
      return { key: command.key, value: command.value ?? null };
    }
    if (command.op === 'delete' && typeof command.key === 'string') {
      const existed = this.map.delete(command.key);
      return { key: command.key, deleted: existed };
    }
    return null;
  }

  get(key) {
    return this.map.has(key) ? this.map.get(key) : undefined;
  }

  has(key) {
    return this.map.has(key);
  }

  keys() {
    return [...this.map.keys()].sort();
  }

  get size() {
    return this.map.size;
  }

  snapshot() {
    return Object.fromEntries(this.map);
  }
}
