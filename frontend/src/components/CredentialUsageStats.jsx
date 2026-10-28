import { useEffect, useState } from "react";
import { Loader2, RefreshCw } from "lucide-react";
import api from "../api";
import Modal from "./Modal";
import { formatMillions } from "../utils/tokens";

const time = value => value ? new Date(value * 1000).toLocaleString("zh-CN") : "—";
function rangeParams(range) {
  const end = Date.now() / 1000;
  const start = range === "today" ? new Date().setHours(0, 0, 0, 0) / 1000 : range === "all" ? null : end - Number(range) * 3600;
  return { ...(start === null ? {} : { start_timestamp: start }), end_timestamp: end };
}
const inputClass = "rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 min-w-0";
const tierStyles = { ultra: "bg-purple-100 text-purple-800", ultra_lite: "bg-purple-100 text-purple-800", pro: "bg-blue-100 text-blue-800", free: "bg-slate-100 text-slate-700" };

export default function CredentialUsageStats() {
  const [range, setRange] = useState("today");
  const [email, setEmail] = useState("");
  const [model, setModel] = useState("");
  const [filters, setFilters] = useState(() => rangeParams("today"));
  const [page, setPage] = useState(1);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [detail, setDetail] = useState(null);
  const [errorPage, setErrorPage] = useState(1);
  const [errors, setErrors] = useState(null);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError("");
    api.get("/usage-logs/credential-stats", { params: { ...filters, offset: (page - 1) * 50, limit: 50 }, signal: controller.signal })
      .then(res => { if (!controller.signal.aborted) setData(res.data); })
      .catch(err => { if (!controller.signal.aborted) { setData(null); setError(err.response?.data?.detail || "加载凭证统计失败"); } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [filters, page]);

  useEffect(() => {
    if (!detail) return;
    const controller = new AbortController();
    setDetailLoading(true); setDetailError(""); setErrors(null);
    api.get("/usage-logs/credential-errors", { params: {
      ...filters, credential_email: detail.credential_email, model_name: detail.model_name,
      offset: (errorPage - 1) * 20, limit: 20,
    }, signal: controller.signal }).then(res => { if (!controller.signal.aborted) setErrors(res.data); })
      .catch(err => { if (!controller.signal.aborted) setDetailError(err.response?.data?.detail || "加载错误详情失败"); })
      .finally(() => { if (!controller.signal.aborted) setDetailLoading(false); });
    return () => controller.abort();
  }, [detail, errorPage, filters]);

  const apply = event => {
    event.preventDefault(); setPage(1);
    setFilters({ ...rangeParams(range), ...(email.trim() ? { credential_email: email.trim() } : {}), ...(model.trim() ? { model_name: model.trim() } : {}) });
  };
  const summary = data?.summary;
  const pages = Math.max(1, Math.ceil((data?.total || 0) / 50));
  return <div className="space-y-4">
    <div className="rounded-xl border border-blue-200 bg-blue-50 p-4 text-sm text-slate-700 space-y-1">
      <h3 className="font-semibold text-blue-800">凭证 × 模型调用统计</h3>
      <p>每个凭证、每个实际调用模型单独计数。上游重试分别计数，成功和失败按请求日志记录汇总。</p>
      <p className="text-xs text-slate-600">凭证级别显示当前保存的 Ultra / Pro / Free；已删除或缺少级别信息的凭证显示“未知”。</p>
      <p className="text-xs text-slate-600">1 M = 1,000,000 Tokens。输入包含缓存命中，输出包含思考 Tokens；上游未返回的用量无法计入。</p>
      <p className="text-xs text-slate-600">统计仅包含保留的请求日志；清理日志会同步减少统计，关闭日志后不会新增统计。后台凭证测试不计入这里。</p>
    </div>
    <form onSubmit={apply} className="flex flex-wrap gap-2">
      <select aria-label="统计时间范围" value={range} onChange={e => setRange(e.target.value)} className={inputClass}>
        <option value="today">今天</option><option value="24">最近 24 小时</option><option value="168">最近 7 天</option><option value="720">最近 30 天</option><option value="all">全部保留日志</option>
      </select>
      <input aria-label="统计凭证邮箱" value={email} onChange={e => setEmail(e.target.value)} placeholder="凭证邮箱（留空查全部）" className={`${inputClass} flex-1`} />
      <input aria-label="统计模型名称" value={model} onChange={e => setModel(e.target.value)} placeholder="完整模型名称（留空查全部）" className={`${inputClass} flex-1`} />
      <button disabled={loading} className="btn btn-primary flex items-center gap-1.5 text-sm disabled:opacity-50"><RefreshCw size={14} /> 查询 / 刷新</button>
    </form>
    <p className="text-xs text-slate-500">查询范围：{filters.start_timestamp ? time(filters.start_timestamp) : "全部保留记录"} 至 {time(filters.end_timestamp)}</p>
    {data?.logging_enabled === false && <p role="alert" className="text-sm text-amber-800 bg-amber-50 rounded-lg p-3">请求日志当前已关闭，以下仅为历史保留数据。</p>}
    {error && <p role="alert" className="text-sm text-red-700">{error}</p>}
    {summary && <>
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        {[['调用总数', summary.total_requests, 'text-slate-900'], ['成功', summary.success_count, 'text-emerald-700'], ['失败', summary.failure_count, 'text-red-700'], ['输入 Tokens', formatMillions(summary.prompt_tokens), 'text-blue-700'], ['输出 Tokens', formatMillions(summary.completion_tokens), 'text-purple-700']].map(([label, count, color]) =>
          <div key={label} className="card p-4 text-center"><div className={`text-xl font-bold ${color}`}>{count}</div><div className="text-xs text-slate-500 mt-1">{label}</div></div>)}
      </div>
      <p className="text-xs text-slate-500">匹配日志实际覆盖：{time(summary.first_request)} 至 {time(summary.last_request)} · {data.total} 个凭证 / 模型组合</p>
    </>}
    {loading ? <div role="status" className="py-10 flex justify-center"><Loader2 className="animate-spin text-purple-700" /></div> : data?.items?.length ? <>
      <div className="table-container"><table>
        <thead><tr>{['凭证', '模型', '调用总数', '成功', '失败', '输入 Tokens (M)', '输出 Tokens (M)', '失败错误码 / 详情'].map(h => <th key={h} className="whitespace-nowrap">{h}</th>)}</tr></thead>
        <tbody>{data.items.map(row => <tr key={JSON.stringify([row.credential_email, row.model_name])}>
          <td className="break-all min-w-48"><div>{row.credential_email || "未分配凭证"}</div>
            {row.credential_email && <span className={`inline-block mt-1 rounded px-2 py-0.5 text-xs font-medium ${tierStyles[row.credential_tier] || "bg-slate-100 text-slate-500"}`}>
              级别：{({ ultra: "Ultra", ultra_lite: "Ultra Lite", pro: "Pro", free: "Free" })[row.credential_tier] || "未知"}
            </span>}
            {row.credential_note && <p className="text-xs text-slate-500 whitespace-pre-wrap mt-1">备注：{row.credential_note}</p>}
          </td>
          <td className="break-all min-w-44">{row.model_name}</td>
          <td>{row.total_requests}</td><td className="text-emerald-700">{row.success_count}</td><td className="text-red-700">{row.failure_count}</td>
          <td title={`${row.prompt_tokens || 0} Tokens`} className="whitespace-nowrap">{formatMillions(row.prompt_tokens)}</td>
          <td title={`${row.completion_tokens || 0} Tokens`} className="whitespace-nowrap">{formatMillions(row.completion_tokens)}</td>
          <td className="min-w-44"><div className="text-xs space-y-1">{row.errors.map(e => <div key={e.status_code}>{e.status_code ? `HTTP ${e.status_code}` : "无状态码"}：{e.count} 次</div>)}</div>
            {row.failure_count > 0 ? <button onClick={() => { setErrorPage(1); setDetail(row); }} className="text-purple-700 text-xs mt-2 underline">查看报错详情</button> : <span className="text-slate-400">—</span>}
          </td>
        </tr>)}</tbody>
      </table></div>
      <div className="flex items-center justify-between text-sm"><span>第 {page} / {pages} 页</span><div className="flex gap-2">
        <button onClick={() => setPage(p => p - 1)} disabled={page <= 1} className="btn btn-secondary disabled:opacity-40">上一页</button>
        <button onClick={() => setPage(p => p + 1)} disabled={page >= pages} className="btn btn-secondary disabled:opacity-40">下一页</button>
      </div></div>
    </> : !error && <div className="card text-center text-sm text-slate-500">所选范围暂无调用记录。仅展示有记录的凭证 / 模型组合。</div>}
    {detail && <Modal title="模型调用失败详情" onClose={() => setDetail(null)}>
      <p>{detail.credential_email || "未分配凭证"}</p><p className="text-xs text-slate-500 mb-3">{detail.model_name}</p>
      {detailLoading && <p>正在加载…</p>}{detailError && <p className="text-red-700">{detailError}</p>}
      {errors?.items?.map((row, i) => <div key={i} className="border border-red-200 bg-red-50 rounded-lg p-3 my-2">
        <p className="font-medium text-red-700">{row.status_code ? `HTTP ${row.status_code}` : "无状态码"} · {row.count} 次</p>
        <pre className="whitespace-pre-wrap break-words text-xs mt-2 font-mono">{row.error_message || "日志未记录错误详情"}</pre>
        <p className="text-xs text-slate-500 mt-2">最近发生：{time(row.last_seen)}</p>
      </div>)}
      {errors && <div className="flex flex-wrap items-center gap-2 text-xs mt-3"><span>{errors.total} 类报错 · 第 {errorPage} 页</span>
        <button disabled={errorPage <= 1} onClick={() => setErrorPage(p => p - 1)} className="btn btn-secondary disabled:opacity-40">上一页</button>
        <button disabled={errorPage * 20 >= errors.total} onClick={() => setErrorPage(p => p + 1)} className="btn btn-secondary disabled:opacity-40">下一页</button>
      </div>}
    </Modal>}
  </div>;
}
