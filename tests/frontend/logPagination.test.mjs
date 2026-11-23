import assert from 'node:assert/strict';
import test from 'node:test';
import { applyLogPage } from '../../frontend/src/utils/logPagination.js';

test('shrinking log results request the last remaining page before displaying rows', () => {
  const pages = [];
  const displayed = [];
  const receive = (page, data) => applyLogPage(data, page, 20,
    value => pages.push(value), value => displayed.push(value));

  receive(3, { total: 22, items: [] });
  const lastPage = { total: 22, items: [21, 22] };
  receive(2, lastPage);
  assert.deepEqual(pages, [2]);
  assert.deepEqual(displayed, [lastPage]);
});

test('empty results reset the offset once and newly added records appear on page one', () => {
  const pages = [];
  const displayed = [];
  const receive = (page, data) => applyLogPage(data, page, 50,
    value => pages.push(value), value => displayed.push(value));

  const empty = { total: 0, items: [] };
  const updated = { total: 1, items: [1] };
  receive(2, empty);
  receive(1, empty);
  receive(1, updated);
  assert.deepEqual(pages, [1]);
  assert.deepEqual(displayed, [empty, updated]);
});
