import test from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PersistentState } from '../src/persistence.js';

async function tmpdir(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'raftkv-persist-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  return dir;
}

test('fresh state loads defaults when no file exists', async (t) => {
  const dir = await tmpdir(t);
  const state = await new PersistentState(dir).load();
  assert.equal(state.currentTerm, 0);
  assert.equal(state.votedFor, null);
  assert.equal(state.log.lastIndex(), 0);
});

test('term, vote and log survive a reload', async (t) => {
  const dir = await tmpdir(t);
  const state = await new PersistentState(dir).load();
  state.currentTerm = 7;
  state.votedFor = 'n2';
  state.log.append(7, { op: 'put', key: 'a', value: 1 });
  state.log.append(7, { op: 'put', key: 'b', value: 2 });
  await state.persist();

  const reloaded = await new PersistentState(dir).load();
  assert.equal(reloaded.currentTerm, 7);
  assert.equal(reloaded.votedFor, 'n2');
  assert.equal(reloaded.log.lastIndex(), 2);
  assert.equal(reloaded.log.lastTerm(), 7);
  assert.deepEqual(reloaded.log.entryAt(1).command, { op: 'put', key: 'a', value: 1 });
});

test('concurrent persist calls do not corrupt the file', async (t) => {
  const dir = await tmpdir(t);
  const state = await new PersistentState(dir).load();
  state.currentTerm = 3;
  await Promise.all([state.persist(), state.persist(), state.persist()]);
  const reloaded = await new PersistentState(dir).load();
  assert.equal(reloaded.currentTerm, 3);
});
