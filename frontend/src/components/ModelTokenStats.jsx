import { Fragment, useEffect, useMemo, useState } from 'react';
import { ChevronRight, Loader2 } from 'lucide-react';
import api from '../api';
import { formatMillions } from '../utils/tokens';
import { estimateQuota, groupModelUsage, percent, quotaPool, defaultEstimateMode } from '../utils/modelUsage';
import { quotaCalibration } from '../utils/quotaCalibration';

const WEIGHTS_KEY = `agy-model-quota-weights-v1:${window.location.pathname}`;
const MODE_KEY = `agy-model-quota-mode-v2:${window.location.pathname}`;
const loadMode = () => {
  try { return defaultEstimateMode(localStorage.getItem(MODE_KEY) || (localStorage.getItem(`agy-model-quota-mode-v1:${window.location.pathname}`) === 'custom' ? 'custom' : null)); }
  catch { return defaultEstimateMode(null); }
};
const time = ts => ts == null ? '—' : new Date(ts * 1000).toLocaleString('zh-CN');
const number = value => Number(value || 0).toLocaleString('zh-CN');
const loadWeights = () => {
  try {
    const data = JSON.parse(localStorage.getItem(WEIGHTS_KEY) || '{}');
    return data && typeof data === 'object' && !Array.isArray(data) ? data : {};
  } catch { return {}; }
};

function UsageCells({ row, estimateMode }) {
  return <><td>{number(row.attempts)}</td>
            <td className="whitespace-nowrap" title={number(row.prompt_tokens)}>{formatMillions(row.prompt_tokens)}<div className="text-xs text-slate-500">{percent(row.prompt_tokens_percentage)}</div></td>
            <td className="whitespace-nowrap" title={number(row.completion_tokens)}>{formatMillions(row.completion_tokens)}<div className="text-xs text-slate-500">{percent(row.completion_tokens_percentage)}</div></td>
            <td className="whitespace-nowrap" title={number(row.cached_tokens)}>{formatMillions(row.cached_tokens)}</td>
            <td className="min-w-40" title={number(row.total_tokens)}><div className="font-semibold whitespace-nowrap">{formatMillions(row.total_tokens)}</div>
              <div className="text-xs text-purple-700">{percent(row.total_tokens_percentage)}</div>
              <div className="bg-purple-100 rounded-full h-1.5 mt-1"><div className="h-full bg-purple-500 rounded-full" style={{ width: `${row.total_tokens_percentage || 0}%` }} /></div></td>
            <td className="whitespace-nowrap" title={number(row.failed_tokens)}>{formatMillions(row.failed_tokens)}</td>
            <td className="min-w-40"><span className="font-medium">{percent(row.estimated_quota_percentage)}</span><div className="text-xs text-slate-500">{row.quota_pool || '未识别额度组'}</div>
              <div className="text-xs text-slate-500">{!row.quota_pool ? '不参与估算' : estimateMode === 'calibrated' ? row.estimate_missing_models ? `${row.estimate_missing_models} 个模型待校准` : row.calibration ? '实测权重 · 缓存按输入估算' : '尚无实测权重' : estimateMode === 'tokens' ? '等权 Token 估算' : row.estimate_missing_models ? `${row.estimate_missing_models} 个模型待配置` : row.estimated_units == null ? '未配置权重' : `${row.estimated_units.toLocaleString('zh-CN', { maximumFractionDigits: 4 })} 估算单位`}</div></td>
  </>;
}

export default function ModelTokenStats({ filters }) {
  const [data, setData] = useState(null), [error, setError] = useState(''), [loading, setLoading] = useState(true);
  const [weights, setWeights] = useState(loadWeights), [storageError, setStorageError] = useState('');
  const [estimateMode, setEstimateMode] = useState(loadMode);
  const [sort, setSort] = useState('total_tokens');
  const [expandedModels, setExpandedModels] = useState(() => new Set());
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError(''); setData(null);
    api.get('/usage-logs/model-tokens', { params: filters, signal: controller.signal })
      .then(res => { if (!controller.signal.aborted) setData(res.data); })
      .catch(err => { if (!controller.signal.aborted) setError(err.response?.data?.detail || '模型用量加载失败'); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [filters]);
  const rows = useMemo(() => estimateQuota(data?.items || [], weights, estimateMode)
    .sort((a, b) => (b[sort] || 0) - (a[sort] || 0) || a.model_name.localeCompare(b.model_name)), [data, weights, sort, estimateMode]);
  const groups = useMemo(() => groupModelUsage(rows, sort), [rows, sort]);
  const toggleModel = model => setExpandedModels(previous => {
    const next = new Set(previous);
    if (next.has(model)) next.delete(model); else next.add(model);
    return next;
  });
  const changeMode = value => {
    setEstimateMode(value);
    try { localStorage.setItem(MODE_KEY, value); setStorageError(''); }
    catch { setStorageError('浏览器未允许保存，设置仅在本次页面打开期间有效。'); }
  };
  const changeWeight = (model, key, value) => {
    const next = { ...weights, [model]: { ...weights[model], [key]: value } };
    setWeights(next);
    try { localStorage.setItem(WEIGHTS_KEY, JSON.stringify(next)); setStorageError(''); }
    catch { setStorageError('浏览器未允许保存，权重仅在本次页面打开期间有效。'); }
  };
  const summary = data?.summary;
  return <details className="rounded-xl border border-purple-200 bg-white p-4 sm:p-5" aria-label="模型 Token 与额度统计">
    <summary className="cursor-pointer font-semibold text-lg text-slate-900">模型 Token 与额度占比</summary>
    <div className="mt-4 space-y-4">
    <div className="flex flex-wrap justify-between items-center gap-3">
      <p className="text-xs text-slate-500">跟随上方时间、邮箱和实际模型筛选 · 占比以筛选后的全部匹配记录为分母</p>
      <select aria-label="模型用量排序" value={sort} onChange={e => setSort(e.target.value)} className="rounded-lg border border-slate-300 p-2 text-sm">
        <option value="total_tokens">按总 Token</option><option value="prompt_tokens">按输入 Token</option><option value="completion_tokens">按输出 Token</option><option value="attempts">按调用次数</option>
      </select>
    </div>
    <p className="text-xs text-slate-600">总 Token 优先采用上游总量，缺失时用输入加输出。输入含缓存、输出含思考，缓存不重复相加。包含成功、失败和重试中已记录的用量；进行中及未返回用量的消耗不在统计内。</p>
    {loading && <p role="status" className="flex gap-2 text-sm text-slate-500"><Loader2 size={18} className="animate-spin" />加载模型用量…</p>}
    {error && <p role="alert" className="text-sm text-red-700">{error}</p>}
    {data?.logging_enabled === false && <p role="alert" className="text-sm text-amber-800">日志记录已关闭，仅展示保留的历史用量。</p>}
    {summary && <>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {[["总 Token", summary.total_tokens], ["输入 Token", summary.prompt_tokens], ["输出 Token", summary.completion_tokens], ["失败调用 Token", summary.failed_tokens]].map(([label, value]) =>
          <div key={label} className="rounded-lg bg-slate-50 p-3"><div className="text-xs text-slate-500">{label}</div><div title={number(value)} className="text-lg font-semibold mt-1">{formatMillions(value)}</div></div>)}
      </div>
      <p className="text-xs text-slate-500">日志实际覆盖：{time(summary.first_request)} 至 {time(summary.last_request)}。{number(summary.attempts)} 次记录中，{number(summary.positive_usage_attempts)} 次记录了正 Token 用量，其余为零或未返回；不等于未产生消耗。</p>
      {!!summary.total_difference_tokens && <p className="text-xs text-slate-500">上游总量与输入加输出的差额为 {number(summary.total_difference_tokens)} Token；保留上游总量，不强行分摊到输入或输出。</p>}
      <div className="rounded-lg bg-amber-50 border border-amber-200 p-3 text-sm text-amber-900 space-y-1">
        <div className="flex flex-wrap gap-3 mb-2" role="group" aria-label="额度估算方式">
          {[["calibrated", "实测权重估算"], ["tokens", "等权 Token 估算"], ["custom", "自定义权重估算"]].map(([value, label]) =>
            <label key={value} className="flex items-center gap-1.5"><input type="radio" name="quota-estimate-mode" value={value} disabled={value === 'calibrated' && !Object.keys(quotaCalibration.rules).length} checked={estimateMode === value} onChange={() => changeMode(value)} />{label}</label>)}
        </div>
        <p>{estimateMode === 'calibrated'
          ? '根据独立调用前后的额度变化校准输入和输出权重，自动计算同一额度组内各模型的估算份额。同组有用量的模型全部完成校准后才显示占比。'
          : estimateMode === 'tokens'
          ? '按同组各模型每个 Token 消耗相同的假设估算：模型总 Token ÷ 同组总 Token。无需配置权重；输入、输出和缓存暂按等权计算，实际模型倍率可能不同。'
          : '使用下方配置的输入、缓存、输出权重估算。同组所有有用量的模型都配置完整后，才显示该组内占比。'}</p>
        <p className="text-xs">仅比较筛选范围内的消耗份额，Gemini 与 Claude / GPT-OSS 分组计算。历史无逐次扣额数据；实测权重是有限样本的近似值，其他模式未校准。占比不代表已用周额度或 5 小时额度的百分比。</p>
        {quotaCalibration.status === 'inconclusive' && <p className="text-xs mt-2"><strong>实测校准未通过（{quotaCalibration.attemptedAt}）</strong>：{quotaCalibration.message}</p>}
        {storageError && <p role="alert" className="text-xs">{storageError}</p>}
      </div>
      {!rows.length ? <p className="py-4 text-sm text-slate-500">所选范围暂无模型用量记录。</p> : <>
        <p className="text-xs text-slate-500">按模型类型汇总，点击模型展开各思考等级。明细占比仍以全部筛选结果为分母；额度占比仍在原额度组内计算。仅合并筛选后可见的实际模型。</p>
        <div className="table-container"><table aria-label="按模型类型汇总的 Token 与额度占比">
          <thead><tr>{['模型类型 / 思考等级', '调用次数', '输入 / 占比', '输出 / 占比', '缓存（已含输入）', '总 Token / 占比', '失败 Token', '额度估算（组内）'].map(h => <th key={h} className="whitespace-nowrap">{h}</th>)}</tr></thead>
          <tbody>{groups.map(group => {
            const expanded = expandedModels.has(group.model_name);
            return <Fragment key={group.model_name}>
              <tr className="bg-purple-50/60">
                <td className="min-w-56">
                  <button type="button" aria-expanded={expanded} aria-label={`${expanded ? '收起' : '展开'} ${group.model_name} 等级明细`}
                    onClick={() => toggleModel(group.model_name)}
                    className="flex w-full items-start gap-2 rounded text-left focus-visible:outline focus-visible:outline-2 focus-visible:outline-purple-500">
                    <ChevronRight size={18} aria-hidden="true" className={`mt-0.5 shrink-0 transition-transform ${expanded ? 'rotate-90' : ''}`} />
                    <span><span className="font-semibold break-all">{group.model_name}</span><span className="block text-xs text-slate-500 mt-1">合计 · {group.variants.length} 个实际模型 · {expanded ? '收起' : '展开'}等级明细</span></span>
                  </button>
                </td>
                <UsageCells row={group} estimateMode={estimateMode} />
              </tr>
              {expanded && group.variants.map(row => <tr key={row.model_name} className="bg-white">
                <td className="min-w-56"><div className="pl-7 border-l-2 border-purple-100 ml-2">
                  <span className="text-sm font-medium text-purple-800">{row.thinking_level || '未标注等级'}</span>
                  <div className="break-all text-xs text-slate-500 mt-1">{row.model_name}</div>
                </div></td>
                <UsageCells row={row} estimateMode={estimateMode} />
              </tr>)}
            </Fragment>;
          })}</tbody>
        </table></div>
        {(estimateMode === 'calibrated' || quotaCalibration.observations?.length > 0) && <details className="rounded-lg border border-slate-200 p-3">
          <summary className="cursor-pointer text-sm font-medium">查看额度校准记录与适用范围</summary>
          <p className="text-xs text-slate-500 my-3">有效校准时间：{quotaCalibration.testedAt || '尚未取得有效结果'}；{quotaCalibration.tier} 账号、短文本请求的周额度百分点 / 百万 Token。输出含思考。缓存折扣未实测，暂按普通输入计；跨套餐、长上下文、图片和视频可能不同。{Object.keys(quotaCalibration.rules).length ? '历史用量按这些权重回算，仅用于份额估算。' : '当前没有有效实测权重。以下调用的 Token 已计入统计，额度读数未用于计算倍率。'}</p>
          {Object.keys(quotaCalibration.rules).length > 0 && <div className="table-container"><table><thead><tr><th>模型</th><th>输入权重</th><th>输出权重</th><th>有效样本</th></tr></thead><tbody>
            {Object.entries(quotaCalibration.rules).map(([model, rule]) => <tr key={model}><td>{model}</td><td>{rule.input}</td><td>{rule.output}</td><td>{rule.samples}</td></tr>)}
          </tbody></table></div>}
          {quotaCalibration.observations?.length > 0 && <div className="table-container"><table><thead><tr><th>实际模型</th><th>测试类型</th><th>输入 / 输出 Token</th><th>调用结果</th><th>校准结果</th></tr></thead><tbody>
            {quotaCalibration.observations.map((r, i) => <tr key={i}><td>{r.model}</td><td>{r.profile}</td><td>{number(r.input)} / {number(r.output)}</td><td>{r.generationSucceeded ? '成功' : '失败'}</td><td>{r.reason}</td></tr>)}
          </tbody></table></div>}
        </details>}
        {estimateMode === 'custom' && <details open className="rounded-lg border border-slate-200 p-3">
          <summary className="cursor-pointer text-sm font-medium">配置额度估算权重（仅保存在当前浏览器）</summary>
          <p className="text-xs text-slate-500 my-3">每百万 Token 的相对消耗单位，留空表示未知。请使用同一比较组内可比较的权重。估算 = 非缓存输入 × 输入权重 + 缓存输入 × 缓存权重 + 输出 × 输出权重；输出已含思考。修改立即应用，不改变服务端额度。</p>
          <div className="space-y-3">{rows.filter(row => quotaPool(row.model_name)).map(row => <div key={row.model_name} className="grid grid-cols-1 sm:grid-cols-4 gap-2 items-center">
            <span className="text-xs break-all">{row.model_name}</span>
            {[["input", "非缓存输入"], ["cache", "缓存输入"], ["output", "输出含思考"]].map(([key, label]) => <label key={key} className="text-xs text-slate-500">{label}<input type="number" min="0" step="any" aria-label={`${row.model_name} ${label}权重`} value={weights[row.model_name]?.[key] ?? ''} onChange={e => changeWeight(row.model_name, key, e.target.value)} placeholder="未知" className="block w-full rounded border border-slate-300 p-2 text-slate-800" /></label>)}
          </div>)}</div>
        </details>}
      </>}
    </>}
    </div>
  </details>;
}
