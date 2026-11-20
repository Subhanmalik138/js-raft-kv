import test from 'node:test';
import assert from 'node:assert/strict';
import { Log } from '../src/log.js';

test('log is 1-indexed with a dummy entry at 0', () => {
  const log = new Log();
  assert.equal(log.lastIndex(), 0);
  assert.equal(log.lastTerm(), 0);
  log.append(1, { op: 'put', key: 'a', value: 1 });
  assert.equal(log.lastIndex(), 1);
  assert.equal(log.lastTerm(), 1);
  assert.equal(log.termAt(0), 0);
  assert.equal(log.termAt(1), 1);
});

test('termAt returns -1 outside the log', () => {
  const log = new Log();
  assert.equal(log.termAt(-1), -1);
  assert.equal(log.termAt(5), -1);
});

test('sliceAfter returns entries after an index', () => {
  const log = new Log();
  log.append(1, 'a');
  log.append(1, 'b');
  log.append(2, 'c');
  assert.deepEqual(log.sliceAfter(1).map((e) => e.command), ['b', 'c']);
  assert.deepEqual(log.sliceAfter(3), []);
  assert.equal(log.sliceAfter(0, 1).length, 1);
});

test('truncateFrom deletes the conflicting tail', () => {
  const log = new Log();
  log.append(1, 'a');
  log.append(1, 'b');
  log.append(1, 'c');
  log.truncateFrom(2);
  assert.equal(log.lastIndex(), 1);
  assert.equal(log.entryAt(1).command, 'a');
  log.append(2, 'x');
  assert.equal(log.lastTerm(), 2);
});

test('JSON round-trip preserves entries', () => {
  const log = new Log();
  log.append(1, { op: 'put', key: 'k', value: 'v' });
  log.append(2, { op: 'delete', key: 'k' });
  const restored = Log.fromJSON(log.toJSON());
  assert.equal(restored.lastIndex(), 2);
  assert.deepEqual(restored.entryAt(2), { term: 2, command: { op: 'delete', key: 'k' } });
});
