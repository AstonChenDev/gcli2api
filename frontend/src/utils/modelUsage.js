import { quotaCalibration } from './quotaCalibration.js';

export const percent = value => value == null ? '—' : value > 0 && value < 0.01 ? '<0.01%' : `${Number(value).toFixed(2)}%`;

// These are comparison groups, not a conversion between independent quota pools.
export function quotaPool(model) {
  if (model.startsWith('gemini-')) return 'Gemini';
  if (/^(claude-|gpt-oss)/.test(model)) return 'Claude / GPT-OSS';
  return null;
}

export function defaultEstimateMode(saved, calibration = quotaCalibration) {
  const available = Object.keys(calibration.rules || {}).length > 0;
  if (saved === 'custom' || saved === 'tokens') return saved;
  return available ? 'calibrated' : 'tokens';
}

export function estimateQuota(rows, weights = {}, mode = 'custom') {
  if (mode === 'calibrated') return estimateCalibratedQuota(rows);
  const groups = new Map();
  const result = rows.map(row => {
    const pool = quotaPool(row.model_name);
    const rule = weights[row.model_name];
    const valid = rule && ['input', 'output', 'cache'].every(k =>
      rule[k] !== '' && rule[k] != null && Number.isFinite(Number(rule[k])) && Number(rule[k]) >= 0);
    const coherent = row.cached_tokens <= row.prompt_tokens && row.cached_tokens >= 0;
    // Equal-token mode uses the upstream total (including any reported difference).
    // It is an uncalibrated proxy, not a claim about provider charging weights.
    const units = !pool ? null : mode === 'tokens'
      ? (Number.isFinite(row.total_tokens) && row.total_tokens >= 0 ? row.total_tokens / 1e6 : null)
      : valid && coherent ? ((row.prompt_tokens - row.cached_tokens) * Number(rule.input)
        + row.cached_tokens * Number(rule.cache) + row.completion_tokens * Number(rule.output)) / 1e6 : null;
    const safeUnits = Number.isFinite(units) ? units : null;
    if (pool) {
      const group = groups.get(pool) || { units: 0, missing: 0 };
      group.units += safeUnits || 0;
      if (safeUnits == null && row.total_tokens > 0) group.missing += 1;
      groups.set(pool, group);
    }
    return { ...row, quota_pool: pool, estimate_basis: mode, estimated_units: safeUnits };
  });
  return result.map(row => {
    const group = groups.get(row.quota_pool);
    return { ...row, estimate_missing_models: group?.missing || 0,
      estimated_quota_percentage: row.estimated_units != null && group && !group.missing && group.units > 0
        && Number.isFinite(group.units) ? row.estimated_units / group.units * 100 : null };
  });
}

export function estimateCalibratedQuota(rows, calibration = quotaCalibration) {
  // Missing models invalidate the whole pool, rather than silently mixing scales.
  const result = estimateQuota(rows, calibration.rules, 'custom');
  return result.map(row => ({ ...row, estimate_basis: 'calibrated',
    calibration: calibration.rules[row.model_name] || null }));
}

export function statsRange(range, startText = '', endText = '', now = Date.now()) {
  if (range === 'custom') {
    const start = new Date(startText).getTime(), end = new Date(endText).getTime();
    if (!startText || !endText || !Number.isFinite(start) || !Number.isFinite(end)) throw new Error('请选择完整的起止时间');
    if (start > end) throw new Error('开始时间不能晚于结束时间');
    return { start_timestamp: start / 1000, end_timestamp: end / 1000 };
  }
  const end = now / 1000;
  const today = new Date(now); today.setHours(0, 0, 0, 0);
  const start = range === 'today' ? today.getTime() / 1000 : range === 'all' ? null : end - Number(range) * 3600;
  return { ...(start == null ? {} : { start_timestamp: start }), end_timestamp: end };
}
