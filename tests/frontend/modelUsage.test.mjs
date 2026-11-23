import test from 'node:test';
import assert from 'node:assert/strict';
import { estimateQuota, estimateCalibratedQuota, groupModelUsage, modelVariant, statsRange, percent } from '../../frontend/src/utils/modelUsage.js';
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

test('model families strip only terminal levels and preserve distinct model types', () => {
  for (const level of ['low', 'medium', 'high', 'xhigh', 'extra-low', 'thinking', 'tiered']) {
    assert.deepEqual(modelVariant(`gemini-4-hydrogen-${level}`), {base:'gemini-4-hydrogen',level});
  }
  assert.equal(modelVariant('claude-opus-5-5-high').base, 'claude-opus-5-5');
  assert.equal(modelVariant('gpt-oss-120b-medium').base, 'gpt-oss-120b');
  for (const model of ['gemini-3.1-flash-lite', 'gemini-3.1-flash-image', 'gemini-4-argon-d', 'gemini-4-argon']) {
    assert.deepEqual(modelVariant(model), {base:model,level:null});
  }
});

const familyRows = [
  {model_name:'gemini-4-argon-high', attempts:2,prompt_tokens:200,completion_tokens:100,cached_tokens:70,total_tokens:330,failed_tokens:40},
  {model_name:'gemini-4-carbon-low', attempts:10,prompt_tokens:300,completion_tokens:200,cached_tokens:100,total_tokens:500,failed_tokens:0},
  {model_name:'gemini-4-argon-low', attempts:1,prompt_tokens:100,completion_tokens:200,cached_tokens:30,total_tokens:300,failed_tokens:20},
  {model_name:'gemini-4-argon-medium', attempts:3,prompt_tokens:200,completion_tokens:100,cached_tokens:50,total_tokens:300,failed_tokens:0},
].map(row => ({...row, prompt_tokens_percentage:row.prompt_tokens/800*100,
  completion_tokens_percentage:row.completion_tokens/600*100,total_tokens_percentage:row.total_tokens/1430*100}));

test('family totals conserve tokens, cached subset, failures and global shares; sort by family totals', () => {
  const estimated = estimateQuota(familyRows, {}, 'tokens');
  const before = structuredClone(estimated);
  const grouped = groupModelUsage(estimated);
  assert.equal(grouped.length, 2);
  const argon = grouped[0];
  assert.equal(argon.model_name, 'gemini-4-argon');
  assert.deepEqual(argon.variants.map(r => r.thinking_level), ['low','medium','high']);
  assert.equal(argon.attempts, 6);
  assert.equal(argon.prompt_tokens, 500);
  assert.equal(argon.completion_tokens, 400);
  assert.equal(argon.cached_tokens, 150);
  assert.equal(argon.total_tokens, 930); // Includes the upstream difference, not input + output + cache.
  assert.equal(argon.failed_tokens, 60);
  assert.equal(argon.prompt_tokens_percentage, 62.5);
  assert.ok(Math.abs(argon.total_tokens_percentage - 930/1430*100) < 1e-9);
  assert.ok(Math.abs(argon.estimated_quota_percentage - 930/1430*100) < 1e-9);
  assert.equal(argon.variants[0].total_tokens_percentage, familyRows[2].total_tokens_percentage);
  assert.equal(groupModelUsage(estimated, 'attempts')[0].model_name, 'gemini-4-carbon');
  assert.deepEqual(estimated, before);
});

test('family quota uses per-level weights and preserves incomplete and zero-denominator states', () => {
  const rules = Object.fromEntries(familyRows.map(row => [row.model_name,{input:1,cache:1,output:1}]));
  rules['gemini-4-argon-high'].output = 10;
  const estimated = estimateCalibratedQuota(familyRows, {rules});
  const argon = groupModelUsage(estimated)[0];
  assert.ok(Math.abs(argon.estimated_units - 0.0018) < 1e-12);
  assert.ok(Math.abs(argon.estimated_quota_percentage - 1800/2300*100) < 1e-9);
  delete rules['gemini-4-argon-low'];
  const incomplete = groupModelUsage(estimateQuota(familyRows, rules));
  assert.ok(incomplete.every(row => row.estimated_quota_percentage === null));
  assert.equal(incomplete[0].estimate_missing_models, 1); // Pool-wide count is not counted once per level.
  assert.equal(incomplete[0].estimated_units, null);
  const zero = groupModelUsage(estimateQuota([{...familyRows[0],total_tokens:0,total_tokens_percentage:null}], {}, 'tokens'))[0];
  assert.equal(zero.total_tokens_percentage, null);
  assert.equal(zero.estimated_quota_percentage, null);
});

test('filtered single levels and unsuffixed records retain their actual identities', () => {
  const filtered = groupModelUsage(estimateQuota([familyRows[0]], {}, 'tokens'));
  assert.equal(filtered[0].variants.length, 1);
  assert.equal(filtered[0].total_tokens, 330);
  const mixed = groupModelUsage(estimateQuota([
    {...familyRows[0],model_name:'gemini-4-argon'},familyRows[0],
    {...familyRows[0],model_name:'claude-sonnet-5-5-high'},
  ], {}, 'tokens'));
  const argon = mixed.find(r => r.model_name === 'gemini-4-argon');
  assert.equal(argon.variants[0].thinking_level, null);
  assert.equal(argon.variants[1].thinking_level, 'high');
  assert.equal(argon.estimated_quota_percentage, 100);
  assert.equal(mixed.find(r => r.model_name === 'claude-sonnet-5-5').estimated_quota_percentage, 100);
  assert.deepEqual(groupModelUsage([]), []);
});
