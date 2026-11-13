import { useCallback, useEffect, useRef, useState } from "react";
import { Copy, Loader2, RefreshCw } from "lucide-react";
import api from "../api";
import { copyText } from "../utils/clipboard";
import Modal from "./Modal";

const formatTime = value => value ? new Date(value * 1000).toLocaleString() : "尚未成功查询";

export default function CredentialModels({ credential, onClose, onUpdated }) {
  const [data, setData] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [search, setSearch] = useState("");
  const [now, setNow] = useState(Date.now());
  const [retryAt, setRetryAt] = useState(0);
  const controller = useRef(null);
  const load = useCallback(async (refresh = false) => {
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    setBusy(true); setError(""); setNotice("");
    try {
      const path = `/creds/models/${encodeURIComponent(credential.email)}`;
      const response = refresh
        ? await api.post(`${path}/refresh`, null, { signal: request.signal })
        : await api.get(path, { signal: request.signal });
      if (request.signal.aborted) return;
      setData(response.data);
      setRetryAt(response.data.retry_after_seconds > 0 ? Date.now() + response.data.retry_after_seconds * 1000 : 0);
      if (refresh) {
        onUpdated?.(response.data);
        setNotice(response.data.refresh_deferred
          ? "已安排后台刷新。稍后点击“重新读取”查看结果。"
          : response.data.failed_checks.length ? "部分信息查询失败，保留上次成功结果。" : "权限已更新。只查询模型目录，没有生成测试内容。");
      }
    } catch (err) {
      if (!request.signal.aborted) {
        if (err.response?.status === 404) setData(null);
        setError(err.response?.data?.detail || "获取模型权限失败，请稍后重试。");
      }
    } finally {
      if (!request.signal.aborted) setBusy(false);
    }
  }, [credential.email, onUpdated]);

  useEffect(() => {
    load();
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => { controller.current?.abort(); clearInterval(timer); };
  }, [load]);

  const copy = async text => {
    try { await copyText(text); setNotice("模型名已复制。"); }
    catch { setError("复制失败，请手动选择模型名复制。"); }
  };
  const models = data?.models || [];
  const visible = models.filter(model => model.name.toLowerCase().includes(search.trim().toLowerCase()));
  const wait = Math.max(0, Math.ceil((retryAt - now) / 1000));

  return <Modal title="可用模型 · 凭证权限" onClose={onClose}>
    <div className="space-y-3">
      <div className="rounded-lg bg-slate-50 p-3 space-y-1">
        <p className="font-medium break-all">{credential.email}</p>
        <p className="text-xs whitespace-pre-wrap">备注：{(data?.note ?? credential.note) || "未设置"}</p>
        <p className="text-xs">会员：{data?.tier || credential.tier} · {(data?.disabled ?? credential.disabled) ? "已禁用" : "已启用"}</p>
      </div>
      <p className="text-xs text-slate-600">以下是此凭证上游允许使用的模型，可复制完整模型名调用。有权限仍可能受额度、冷却或服务负载限制；剩余额度请查看凭证的“额度”。</p>
      {(data?.disabled ?? credential.disabled) && <p className="text-xs text-amber-800">此凭证已禁用，不会参与请求分配；这里仍可查看模型权限。</p>}
      <div className="flex flex-wrap gap-2 items-center">
        <button onClick={() => load(true)} disabled={busy || wait > 0} className="btn btn-primary text-xs flex items-center gap-1 disabled:opacity-50">
          <RefreshCw size={13} className={busy ? "animate-spin" : ""} /> {wait > 0 ? `${wait} 秒后可刷新权限` : "刷新权限"}
        </button>
        <button onClick={() => load()} disabled={busy} className="btn btn-secondary text-xs">重新读取</button>
      </div>
      <div className="text-xs text-slate-500 space-y-1">
        <p>上次成功查询：{formatTime(data?.checked_at)}</p>
        {data?.stale && data.models_known && <p className="text-amber-800">当前为历史结果，等待刷新。</p>}
        {data?.refresh_in_progress && <p>后台正在刷新权限。</p>}
        {data?.failed_checks?.length > 0 && <p className="text-amber-800">最近查询异常：{data.failed_checks.map(kind => ({ auth: "凭证认证", tier: "会员等级", models: "模型目录" }[kind])).join("、")}。已有结果不代表当前实时状态。</p>}
      </div>
      {busy && <p role="status" className="text-xs flex items-center gap-1"><Loader2 size={13} className="animate-spin" /> 正在查询…</p>}
      {error && <p role="alert" className="text-xs text-red-700">{error}</p>}
      {notice && <p role="status" className="text-xs text-purple-700">{notice}</p>}
      {data && !data.models_known && <p className="text-sm">尚无有效模型目录，无法判断权限。请点击“刷新权限”。</p>}
      {data?.models_known && <>
        <div className="flex items-center justify-between gap-2">
          <span className="text-xs">支持 {models.length} 个模型</span>
          <button onClick={() => copy(visible.map(model => model.name).join("\n"))} disabled={!visible.length} className="text-xs text-purple-700 disabled:opacity-40">复制筛选结果</button>
        </div>
        <input aria-label="搜索模型" value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索模型，例如 hydrogen、claude" className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm" />
        {!models.length ? <p>上游目录已查询成功，未列出任何可用模型。</p> : !visible.length ? <p>没有匹配的模型。</p> : <ul className="space-y-2">
          {visible.map(model => <li key={model.name} className="rounded-lg border border-slate-200 p-2">
            <div className="flex items-start justify-between gap-2">
              <code className="text-xs break-all select-all">{model.name}</code>
              <button onClick={() => copy(model.name)} aria-label={`复制 ${model.name}`} className="text-purple-700 shrink-0 p-1"><Copy size={13} /></button>
            </div>
            <p className="text-xs mt-1 text-slate-500">{model.quota_cooldown_until * 1000 > now ? `有权限 · 配额冷却至 ${formatTime(model.quota_cooldown_until)}` : "有模型权限"}</p>
          </li>)}
        </ul>}
      </>}
    </div>
  </Modal>;
}
