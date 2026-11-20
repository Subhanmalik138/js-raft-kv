import test from 'node:test';
import assert from 'node:assert/strict';
import { KVStore } from '../src/store.js';

test('put and get round-trip', () => {
  const store = new KVStore();
  store.apply({ op: 'put', key: 'name', value: 'raft' });
  assert.equal(store.get('name'), 'raft');
  assert.equal(store.size, 1);
});

test('get returns undefined for a missing key', () => {
  const store = new KVStore();
  assert.equal(store.get('nope'), undefined);
  assert.equal(store.has('nope'), false);
});

test('put overwrites the previous value', () => {
  const store = new KVStore();
  store.apply({ op: 'put', key: 'k', value: 1 });
  store.apply({ op: 'put', key: 'k', value: 2 });
  assert.equal(store.get('k'), 2);
  assert.equal(store.size, 1);
});

test('delete removes the key and reports whether it existed', () => {
  const store = new KVStore();
  store.apply({ op: 'put', key: 'k', value: 'v' });
  assert.deepEqual(store.apply({ op: 'delete', key: 'k' }), { key: 'k', deleted: true });
  assert.deepEqual(store.apply({ op: 'delete', key: 'k' }), { key: 'k', deleted: false });
  assert.equal(store.size, 0);
});

test('values can be any JSON value, including objects', () => {
  const store = new KVStore();
  store.apply({ op: 'put', key: 'cfg', value: { retries: 3, tags: ['a'] } });
  assert.deepEqual(store.get('cfg'), { retries: 3, tags: ['a'] });
});

test('unknown commands are ignored', () => {
  const store = new KVStore();
  assert.equal(store.apply({ op: 'explode' }), null);
  assert.equal(store.apply(null), null);
  assert.equal(store.size, 0);
});

test('keys are sorted and snapshot is a plain object', () => {
  const store = new KVStore();
  store.apply({ op: 'put', key: 'b', value: 2 });
  store.apply({ op: 'put', key: 'a', value: 1 });
  assert.deepEqual(store.keys(), ['a', 'b']);
  assert.deepEqual(store.snapshot(), { a: 1, b: 2 });
});
