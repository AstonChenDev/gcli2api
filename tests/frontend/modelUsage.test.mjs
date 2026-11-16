import test from 'node:test';
import assert from 'node:assert/strict';
import { estimateQuota, statsRange, percent } from '../../frontend/src/utils/modelUsage.js';
const rows = [
  {model_name:'gemini-a', prompt_tokens:100, completion_tokens:100, cached_tokens:50,total_tokens:200},
  {model_name:'gemini-b', prompt_tokens:100, completion_tokens:100, cached_tokens:0,total_tokens:200},
  {model_name:'claude-a', prompt_tokens:100, completion_tokens:0, cached_tokens:0,total_tokens:100},
];
test('quota is unknown without weights, incomplete group never shows misleading shares', () => {
  assert.ok(estimateQuota(rows, {}).every(r => r.estimated_quota_percentage === null));
  const result = estimateQuota(rows, {'gemini-a':{input:1,output:2,cache:0}});
  assert.equal(result[0].estimated_quota_percentage, null);
  assert.equal(result[0].estimate_missing_models, 1);
});
test('cache is not double charged; independent groups are not mixed', () => {
  const result = estimateQuota(rows, Object.fromEntries(rows.map(r => [r.model_name,{input:1,output:2,cache:0}])));
  assert.equal(result[0].estimated_units, 0.00025);
  assert.ok(Math.abs(result[0].estimated_quota_percentage - 250/550*100) < 1e-9);
  assert.equal(result[2].estimated_quota_percentage, 100);
});
test('empty, invalid and zero weights cannot invent percentages', () => {
  for (const value of ['', -1, 'Infinity', 'wrong', null]) {
    const result = estimateQuota([rows[0]], {'gemini-a':{input:value,output:1,cache:0}});
    assert.equal(result[0].estimated_quota_percentage, null);
  }
  assert.equal(estimateQuota([rows[0]], {'gemini-a':{input:0,output:0,cache:0}})[0].estimated_quota_percentage, null);
  assert.equal(percent(null), '—'); assert.equal(percent(0), '0.00%');
});
test('time ranges are bounded and custom dates are validated', () => {
  const now = new Date('2026-11-16T12:00:00+08:00').getTime();
  assert.deepEqual(statsRange('all','','',now), {end_timestamp:now/1000});
  assert.equal(statsRange('24','','',now).start_timestamp, now/1000 - 86400);
  assert.equal(statsRange('custom','2026-11-15T00:00','2026-11-16T00:00').end_timestamp - statsRange('custom','2026-11-15T00:00','2026-11-16T00:00').start_timestamp, 86400);
  assert.throws(() => statsRange('custom','',''));
  assert.throws(() => statsRange('custom','2026-11-16T00:00','2026-11-15T00:00'));
});

test('automatic equal-token estimate needs no weights and keeps quota pools separate', () => {
  const result = estimateQuota(rows, {}, 'tokens');
  assert.equal(result[0].estimated_quota_percentage, 50);
  assert.equal(result[1].estimated_quota_percentage, 50);
  assert.equal(result[2].estimated_quota_percentage, 100);
  assert.equal(result[0].estimate_missing_models, 0);
  // Saved custom weights must not silently alter equal-token estimates.
  assert.equal(estimateQuota(rows, {'gemini-a': {input:100,output:100,cache:100}}, 'tokens')[0].estimated_quota_percentage, 50);
});
test('automatic estimates preserve reported total, null zero denominators and unknown models', () => {
  const data = [
    {...rows[0], total_tokens:300}, {...rows[1], total_tokens:0},
    {...rows[2], model_name:'unknown', total_tokens:100},
  ];
  const result = estimateQuota(data, {}, 'tokens');
  assert.equal(result[0].estimated_quota_percentage, 100);
  assert.equal(result[1].estimated_quota_percentage, 0);
  assert.equal(result[2].estimated_quota_percentage, null);
  assert.equal(estimateQuota([data[1]], {}, 'tokens')[0].estimated_quota_percentage, null);
});

test('calibrated shares use measured input/output rates and cache proxy, not token shares', async () => {
  const { estimateCalibratedQuota } = await import('../../frontend/src/utils/modelUsage.js');
  const calibrated = estimateCalibratedQuota(rows, {rules: {
    'gemini-a':{input:1,output:3,cache:1,samples:2},
    'gemini-b':{input:2,output:6,cache:2,samples:2},
  }});
  assert.ok(Math.abs(calibrated[0].estimated_quota_percentage - 100/3) < 1e-10);
  assert.ok(Math.abs(calibrated[1].estimated_quota_percentage - 200/3) < 1e-10);
  assert.equal(calibrated[0].estimate_basis, 'calibrated');
  assert.equal(calibrated[0].calibration.samples, 2);
  assert.equal(calibrated[2].estimated_quota_percentage, null);
});

test('calibration gaps invalidate the affected pool without inventing weights', async () => {
  const { estimateCalibratedQuota } = await import('../../frontend/src/utils/modelUsage.js');
  const calibrated = estimateCalibratedQuota(rows, {rules: {
    'gemini-a':{input:1,output:3,cache:1,samples:2},
    'claude-a':{input:10,output:20,cache:10,samples:2},
  }});
  assert.equal(calibrated[0].estimated_quota_percentage, null);
  assert.equal(calibrated[0].estimate_missing_models, 1);
  assert.equal(calibrated[1].calibration, null);
  assert.equal(calibrated[2].estimated_quota_percentage, 100);
});

test('failed or empty calibration falls back to an explicit estimate, retaining custom mode', async () => {
  const { defaultEstimateMode } = await import('../../frontend/src/utils/modelUsage.js');
  const empty = {rules:{}};
  assert.equal(defaultEstimateMode(null, empty), 'tokens');
  assert.equal(defaultEstimateMode('calibrated', empty), 'tokens');
  assert.equal(defaultEstimateMode('custom', empty), 'custom');
  assert.equal(defaultEstimateMode(null, {rules:{'gemini-a':{input:1,output:2,cache:1}}}), 'calibrated');
});
