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

const THINKING_LEVELS = ['none', 'minimal', 'extra-low', 'low', 'medium', 'high', 'xhigh', 'thinking', 'tiered'];
const SUM_FIELDS = ['attempts', 'success_count', 'prompt_tokens', 'completion_tokens',
  'cached_tokens', 'total_tokens', 'total_difference_tokens', 'failed_tokens', 'positive_usage_attempts'];

export function modelVariant(model) {
  // Strip only a terminal thinking/routing level; keep lite, image and version names intact.
  const match = model.match(/^(.*?)-(extra-low|xhigh|high|medium|low|minimal|none|thinking|tiered)$/);
  return { base: match ? match[1] : model, level: match ? match[2] : null };
}

export function groupModelUsage(rows, sort = 'total_tokens') {
  const groups = new Map();
  for (const row of rows) {
    const { base, level } = modelVariant(row.model_name);
    if (!groups.has(base)) groups.set(base, []);
    groups.get(base).push({ ...row, thinking_level: level });
  }
  return [...groups].map(([model_name, variants]) => {
    // Estimate each actual model first, then sum its contribution. Never average
    // percentages or apply one level's quota weight to the entire model family.
    const sumKnown = (key, allowEmpty = false) => {
      const known = variants.filter(row => Number.isFinite(row[key]));
      return known.length && variants.every(row => Number.isFinite(row[key]) || (allowEmpty && row.total_tokens === 0))
        ? known.reduce((sum, row) => sum + row[key], 0) : null;
    };
    const pools = new Set(variants.map(row => row.quota_pool));
    const result = {
      model_name, variants: variants.sort((a, b) =>
        THINKING_LEVELS.indexOf(a.thinking_level) - THINKING_LEVELS.indexOf(b.thinking_level)
        || a.model_name.localeCompare(b.model_name)),
      quota_pool: pools.size === 1 ? variants[0].quota_pool : null,
      estimate_missing_models: Math.max(...variants.map(row => row.estimate_missing_models || 0)),
      estimated_units: sumKnown('estimated_units', true),
      estimated_quota_percentage: sumKnown('estimated_quota_percentage', true),
      calibration: variants.every(row => row.calibration || row.total_tokens === 0),
    };
    for (const key of SUM_FIELDS) result[key] = variants.reduce((sum, row) => sum + (row[key] || 0), 0);
    for (const key of ['prompt_tokens', 'completion_tokens', 'total_tokens']) {
      result[key + '_percentage'] = sumKnown(key + '_percentage');
    }
    return result;
  }).sort((a, b) => (b[sort] || 0) - (a[sort] || 0) || a.model_name.localeCompare(b.model_name));
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
