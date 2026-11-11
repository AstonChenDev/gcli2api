import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { location: { pathname: '/' } };
globalThis.localStorage = { getItem: () => 'synthetic-token' };
const { default: api } = await import('../../frontend/src/api.js');
const { uploadCredentialFiles } = await import('../../frontend/src/utils/credentialUpload.js');

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
