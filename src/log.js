// The replicated log. Entries are 1-indexed, exactly like the Raft paper:
// index 0 is a dummy entry so log[1] is the first real entry.
export class Log {
  constructor(entries = []) {
    // entries[0] is always the dummy { term: 0, command: null }.
    this.entries = [{ term: 0, command: null }, ...entries];
  }

  get length() {
    return this.entries.length - 1;
  }

  lastIndex() {
    return this.entries.length - 1;
  }

  lastTerm() {
    return this.entries[this.entries.length - 1].term;
  }

  termAt(index) {
    if (index < 0 || index >= this.entries.length) return -1;
    return this.entries[index].term;
  }

  entryAt(index) {
    return this.entries[index] ?? null;
  }

  append(term, command) {
    this.entries.push({ term, command });
    return this.lastIndex();
  }

  // Entries after `index` (exclusive), up to `limit` entries.
  sliceAfter(index, limit = Infinity) {
    return this.entries.slice(index + 1, index + 1 + limit).map((e) => ({ ...e }));
  }

  // Delete every entry from `index` onwards (used when a follower's log
  // conflicts with the leader's at that index).
  truncateFrom(index) {
    this.entries.length = index;
  }

  toJSON() {
    return this.entries.slice(1);
  }

  static fromJSON(entries) {
    return new Log(entries ?? []);
  }
}
