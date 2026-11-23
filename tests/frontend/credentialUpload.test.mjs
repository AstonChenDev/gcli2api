import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { location: { pathname: '/' } };
globalThis.localStorage = { getItem: () => 'synthetic-token' };
const { default: api } = await import('../../frontend/src/api.js');
const { appendCredentialFiles, uploadCredentialFiles } = await import('../../frontend/src/utils/credentialUpload.js');

test('preserve same-name files with distinct bytes across picks and drop exact repeats', async () => {
  const prefix = new Uint8Array(300_000);
  const entry = (id, tail, name = 'credentials.zip') => ({
    id, file: new File([prefix, tail], name, { lastModified: 123 }),
  });
  const first = entry(1, 'a');
  const second = entry(2, 'b');
  const other = entry(3, 'a', 'other.zip');
  const current = [first];
  const merged = await appendCredentialFiles(current, [second, entry(4, 'a'), other, second]);
  assert.deepEqual(merged.map(item => item.id), [1, 2, 3]);
  assert.deepEqual(current, [first]);
});

test('keep both entries when a same-name file cannot be read for comparison', async () => {
  const file = new File(['x'], 'creds.json');
  const unreadable = { name: file.name, size: file.size, slice() { throw Error('file changed'); } };
  const merged = await appendCredentialFiles([{ id: 1, file }], [{ id: 2, file: unreadable }]);
  assert.deepEqual(merged.map(item => item.id), [1, 2]);
});

for (const base of ['', '/', '/preview', '/preview/', '/nested/panel/']) {
  test(`credential upload keeps the correct origin and path: ${base || '(empty)'}`, async () => {
    api.defaults.baseURL = base;
    const events = [];
    const form = new FormData();
    form.append('files', new Blob(['{}'], { type: 'application/json' }), 'synthetic.json');
    const controller = new AbortController();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, options) => {
      const resolved = new URL(url, 'http://panel.invalid/');
      assert.equal(resolved.origin, 'http://panel.invalid');
      assert.equal(resolved.pathname, `${base.replace(/\/$/, '')}/creds/upload-stream`);
      assert.equal(options.method, 'POST');
      assert.equal(options.body, form);
      assert.equal(options.signal, controller.signal);
      assert.equal(options.headers.Authorization, 'Bearer synthetic-token');
      return new Response('{"type":"start"}\n{"type":"complete","uploaded_count":1}\n');
    };
    try {
      await uploadCredentialFiles(form, { signal: controller.signal, onEvent: event => events.push(event) });
      assert.deepEqual(events.map(event => event.type), ['start', 'complete']);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
}

test('upload uses its panel login and a late denial leaves a newer login intact', async t => {
  const key = 'agy2apiPanelToken:/';
  const storage = new Map([[key, 'root-login'], ['agy2apiPanelToken:/preview/', 'other-login']]);
  t.mock.method(localStorage, 'getItem', name => storage.get(name) ?? null);
  let authorization;
  t.mock.method(globalThis, 'fetch', async (_url, options) => {
    authorization = options.headers.Authorization;
    storage.set(key, 'renewed');
    return new Response('{}', { status: 401 });
  });
  await assert.rejects(uploadCredentialFiles(new FormData(), { onEvent() {} }), /401/);
  assert.equal(authorization, 'Bearer root-login');
  assert.equal(storage.get(key), 'renewed');
  assert.equal(storage.get('agy2apiPanelToken:/preview/'), 'other-login');
});
