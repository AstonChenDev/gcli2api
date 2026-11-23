import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { location: { pathname: '/preview/' } };
const { default: api } = await import('../../frontend/src/api.js');
const { getPanelToken, panelTokenKey } = await import('../../frontend/src/panelSession.js');

test('upstream credential 401 preserves the panel session, rejected panel bearer clears it', async () => {
  for (const url of ['/creds/quota/test', '/creds/test/test']) {
    for (const rejectedByPanel of [false, true]) {
      let token = 'test-token';
      window.location.href = '/preview/current';
      globalThis.localStorage = {
        getItem: () => token,
        removeItem: () => { token = null; },
      };
      const response = { status: 401, data: rejectedByPanel ? { detail: '密码错误' } : { success: false, email: 'test' } };
      await assert.rejects(api.get(url, { adapter: config => Promise.reject({ config, response }) }));
      assert.equal(token, rejectedByPanel ? null : 'test-token');
      assert.equal(window.location.href, rejectedByPanel ? '/preview/' : '/preview/current');
    }
  }
});

test('legacy and sibling panel passwords are never attached to requests', async () => {
  const storage = new Map([['agy2apiPreviewPanelToken', 'legacy'], ['agy2apiPanelToken:/isolated/', 'other']]);
  globalThis.localStorage = { getItem: key => storage.get(key) ?? null };
  assert.equal(panelTokenKey, 'agy2apiPanelToken:/preview/');
  assert.equal(getPanelToken(), null);
  await api.get('/proxies', { adapter: async config => {
    assert.equal(config.headers.Authorization, undefined);
    return { config, data: {}, status: 200 };
  } });
});

test('a rejected login and a late 401 from a previous session preserve the current login', async () => {
  const storage = new Map([[panelTokenKey, 'old']]);
  globalThis.localStorage = { getItem: key => storage.get(key) ?? null, removeItem: key => storage.delete(key) };
  window.location.href = '/preview/current';
  let sent;
  await assert.rejects(api.post('/auth/login', {}, { adapter: async config => {
    sent = config.headers.Authorization;
    throw { config, response: { status: 401 } };
  } }));
  assert.equal(sent, undefined);
  assert.equal(getPanelToken(), 'old');
  await assert.rejects(api.get('/proxies', { adapter: async config => {
    sent = config.headers.Authorization;
    storage.set(panelTokenKey, 'new');
    throw { config, response: { status: 401 } };
  } }));
  assert.equal(sent, 'Bearer old');
  assert.equal(getPanelToken(), 'new');
  assert.equal(window.location.href, '/preview/current');
});
