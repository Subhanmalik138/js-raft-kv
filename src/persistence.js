import { promises as fs } from 'node:fs';
import path from 'node:path';
import { Log } from './log.js';

// Durable Raft state: currentTerm, votedFor and the log must survive a
// crash, or a restarted node could vote twice in one term and break the
// election-safety guarantee. Writes are atomic (tmp file + rename) and
// serialised through a promise chain so concurrent persist() calls cannot
// interleave.
export class PersistentState {
  constructor(dir) {
    this.dir = dir;
    this.file = path.join(dir, 'state.json');
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = new Log();
    this.#chain = Promise.resolve();
  }

  #chain;

  async load() {
    try {
      const raw = await fs.readFile(this.file, 'utf8');
      const data = JSON.parse(raw);
      this.currentTerm = data.currentTerm ?? 0;
      this.votedFor = data.votedFor ?? null;
      this.log = Log.fromJSON(data.log ?? []);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      // No state on disk yet: fresh node.
    }
    return this;
  }

  persist() {
    this.#chain = this.#chain.then(() => this.#write());
    return this.#chain;
  }

  async #write() {
    await fs.mkdir(this.dir, { recursive: true });
    const tmp = `${this.file}.tmp-${process.pid}`;
    const data = JSON.stringify({
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      log: this.log.toJSON(),
    });
    await fs.writeFile(tmp, data, 'utf8');
    await fs.rename(tmp, this.file);
  }
}
