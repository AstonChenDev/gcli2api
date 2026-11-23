import assert from 'node:assert/strict';
import test from 'node:test';
import { configChanges } from '../../frontend/src/utils/configChanges.js';

test('saving one setting omits untouched rules, passwords, and locked fields', () => {
  const saved = { rules: [{ name: 'sample', patterns: ['a*'] }], panel_password: 'example', port: 4000, retries: 3 };
  const draft = { ...structuredClone(saved), port: 8000, retries: 0 };
  assert.deepEqual(configChanges(saved, draft, ['port']), { retries: 0 });
});

test('preserves explicit clears and disabling switches, omits reverted edits', () => {
  const saved = { enabled: true, url: 'http://example.invalid', rules: [['first', 'second']] };
  assert.deepEqual(configChanges(saved, { enabled: false, url: '', rules: [] }, []),
    { enabled: false, url: '', rules: [] });
  assert.deepEqual(configChanges(saved, structuredClone(saved), []), {});
});
