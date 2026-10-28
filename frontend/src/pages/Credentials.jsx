import {
  AlertCircle,
  Ban,
  BarChart3,
  CheckCircle,
  ChevronDown,
  ChevronUp,
  Clock,
  Download,
  Loader2,
  RefreshCw,
  Shield,
  Snowflake,
  Trash2,
  Zap,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import Modal from "../components/Modal";
import Select from "../components/Select";
import api from "../api";
import QuotaSummary, { quotaGroups } from "../components/QuotaSummary";

export default function Credentials() {
  const [items, setItems] = useState([]);
  const [stats, setStats] = useState({ total: 0, normal: 0, disabled: 0 });
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [statusFilter, setStatusFilter] = useState("all");
  const [errorCodeFilter, setErrorCodeFilter] = useState("all");
  const [cooldownFilter, setCooldownFilter] = useState("all");
  const [tierFilter, setTierFilter] = useState("all");
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(new Set());
  const [message, setMessage] = useState(null);
  const [errorModal, setErrorModal] = useState(null);
  const [busyFiles, setBusyFiles] = useState(new Set());
  const [now, setNow] = useState(Date.now() / 1000);
  // 额度预览相关状态
  const [quotaModal, setQuotaModal] = useState(null); // { email } 额度弹窗
  const [quotaCache, setQuotaCache] = useState({}); // 缓存额度数据 { email: { claude, gemini } | { error } }

  const [quotaLoading, setQuotaLoading] = useState(new Set());
  const quotaCacheRef = useRef({});
  const quotaRequests = useRef(new Map());
  const quotaController = useRef(new AbortController());
  const [testModel, setTestModel] = useState("gemini-4-argon-medium");
  const [batchProgress, setBatchProgress] = useState(null);
  const [testResults, setTestResults] = useState({});
  const stopBatch = useRef(false);
  const [batchRunning, setBatchRunning] = useState(false);

  useEffect(() => {
    quotaController.current = new AbortController();
    return () => { quotaController.current.abort(); stopBatch.current = true; };
  }, []);

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(t);
  }, []);

  const fetchCreds = useCallback(async () => {
    setLoading(true);
    try {
      const offset = (page - 1) * pageSize;
      const res = await api.get("/creds/status", {
        params: {
          offset,
          limit: pageSize,
          status_filter: statusFilter,
          error_code_filter: errorCodeFilter,
          cooldown_filter: cooldownFilter,
          tier_filter: tierFilter,
        },
      });
      setItems(res.data.items);
      setTotal(res.data.total);
      setStats(res.data.stats || { total: 0, normal: 0, disabled: 0 });
      setSelected(new Set());
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "加载凭证失败");
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, statusFilter, errorCodeFilter, cooldownFilter, tierFilter]);

  useEffect(() => {
    fetchCreds();
  }, [fetchCreds]);

  const showMessage = useCallback((type, text, title) => {
    setMessage({ type, text, title: title || (type === "success" ? "操作成功" : type === "error" ? "操作失败" : "提示") });
  }, []);

  const setBusy = (email, busy) => {
    setBusyFiles((prev) => {
      const next = new Set(prev);
      if (busy) next.add(email);
      else next.delete(email);
      return next;
    });
  };

  const doAction = async (email, action) => {
    setBusy(email, true);
    try {
      const res = await api.post("/creds/action", { email, action });
      showMessage("success", res.data.message);
      fetchCreds();
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "操作失败");
    } finally {
      setBusy(email, false);
    }
  };

  const doBatchAction = async (action) => {
    if (selected.size === 0) return;
    setBusy("__batch__", true);
    try {
      const res = await api.post("/creds/batch-action", {
        action,
        emails: Array.from(selected),
      });
      showMessage(res.data.errors?.length ? "error" : "success", res.data.message);
      fetchCreds();
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "批量操作失败");
    } finally {
      setBusy("__batch__", false);
    }
  };

  const verifyProject = async (email) => {
    setBusy(email, true);
    try {
      const res = await api.post(`/creds/verify-project/${encodeURIComponent(email)}`);
      if (res.data.success) {
        showMessage(
          "success",
          `Project=${res.data.project_id}, Tier=${res.data.subscription_tier}`,
          `检验成功: ${email}`
        );
      } else {
        showMessage("error", res.data.message || res.data.error || "检验失败", `检验失败: ${email}`);
      }
      fetchCreds();
    } catch (err) {
      showMessage(
        "error",
        err.response?.data?.detail || err.response?.data?.error || err.response?.data?.message || "检验失败",
        `检验失败: ${email}`
      );
    } finally {
      setBusy(email, false);
    }
  };

  const runTest = async (email, model) => {
    setBusy(email, true);
    let result;
    try {
      const res = await api.post(`/creds/test/${encodeURIComponent(email)}`, null, {
        params: { model }, timeout: 75000,
      });
      result = { email, model, status: res.data.status_code || res.status, message: res.data.message || "测试成功" };
    } catch (err) {
      const data = err.response?.data || {};
      const raw = data.error || data.detail || data.message || (err.code === "ECONNABORTED" ? "测试超时" : "网络请求失败");
      result = { email, model, status: err.response?.status || 0, message: typeof raw === "string" ? raw : JSON.stringify(raw) };
    } finally { setBusy(email, false); }
    setTestResults(prev => ({ ...prev, [email]: result }));
    return result;
  };

  const testCredential = async (email) => {
    if (!testModel.trim()) return showMessage("error", "请填写测试模型");
    const result = await runTest(email, testModel.trim());
    showMessage(result.status === 200 ? "success" : result.status === 429 ? "info" : "error",
      `模型：${result.model}\nHTTP ${result.status || "—"}\n${result.message}`, `测试结果：${email}`);
    fetchCreds();
  };

  const batchTest = async () => {
    if (!selected.size || !testModel.trim() || batchRunning) return;
    const queue = [...selected];
    const model = testModel.trim();
    let index = 0;
    stopBatch.current = false;
    setBatchRunning(true);
    setBusy("__batch__", true);
    setBatchProgress({ total: queue.length, done: 0, success: 0, limited: 0, failed: 0, results: [] });
    try {
      await Promise.all(Array.from({ length: Math.min(3, queue.length) }, async () => {
        while (!stopBatch.current && index < queue.length) {
          const result = await runTest(queue[index++], model);
          setBatchProgress(prev => ({ ...prev, done: prev.done + 1,
            success: prev.success + (result.status === 200 ? 1 : 0),
            limited: prev.limited + (result.status === 429 ? 1 : 0),
            failed: prev.failed + (![200, 429].includes(result.status) ? 1 : 0),
            results: [...prev.results, result],
          }));
        }
      }));
      await fetchCreds();
    } finally { setBatchRunning(false); setBusy("__batch__", false); }
  };

  const requestQuota = useCallback((email, force = false) => {
    const cached = quotaCacheRef.current[email];
    if (!force && cached && Date.now() - cached.updatedAt < 60000) return Promise.resolve();
    if (quotaRequests.current.has(email)) return quotaRequests.current.get(email);
    setQuotaLoading(prev => new Set([...prev, email]));
    const signal = quotaController.current.signal;
    const task = (async () => {
      let value;
      try {
        const res = await api.get(`/creds/quota/${encodeURIComponent(email)}`, { timeout: 75000, signal });
        value = res.data.success ? { groups: quotaGroups(res.data) } : { error: res.data.error || "获取额度失败" };
      } catch (err) {
        if (signal.aborted) return;
        value = { error: err.response?.data?.detail || err.response?.data?.error || "额度查询失败，点击刷新重试" };
      } finally {
        quotaRequests.current.delete(email);
        if (!signal.aborted) setQuotaLoading(prev => { const next = new Set(prev); next.delete(email); return next; });
      }
      if (signal.aborted) return;
      value.updatedAt = Date.now();
      quotaCacheRef.current[email] = value;
      setQuotaCache(prev => ({ ...prev, [email]: value }));
    })();
    quotaRequests.current.set(email, task);
    return task;
  }, []);

  useEffect(() => {
    let cancelled = false;
    let index = 0;
    const emails = items.filter(item => !item.disabled).map(item => item.email);
    const workers = Array.from({ length: Math.min(3, emails.length) }, async () => {
      while (!cancelled && index < emails.length) await requestQuota(emails[index++]);
    });
    Promise.all(workers);
    return () => { cancelled = true; };
  }, [items, requestQuota]);

  const refreshPageQuotas = async () => {
    setBusy("__quota__", true);
    let index = 0;
    const emails = items.filter(item => !item.disabled).map(item => item.email);
    try {
      await Promise.all(Array.from({ length: Math.min(3, emails.length) }, async () => {
        while (index < emails.length) await requestQuota(emails[index++], true);
      }));
    } finally { setBusy("__quota__", false); }
  };

  const openQuotaModal = (email) => { setQuotaModal({ email }); requestQuota(email, true); };

  const downloadSelected = async () => {
    if (!selected.size) return;
    setBusy("__batch__", true);
    try {
      const res = await api.post("/creds/download-selected", { emails: [...selected] }, { responseType: "blob" });
      const url = URL.createObjectURL(res.data);
      const a = document.createElement("a");
      a.href = url; a.download = "antigravity_selected_credentials.zip";
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      showMessage("success", `已打包导出选中的 ${selected.size} 个凭证`);
    } catch (err) {
      let detail = "选中凭证导出失败";
      try { detail = JSON.parse(await err.response.data.text()).detail || detail; } catch { /* fallback */ }
      showMessage("error", detail);
    } finally { setBusy("__batch__", false); }
  };

  const exportCredential = async (email) => {
    try {
      const res = await api.get(`/creds/export/${encodeURIComponent(email)}`, {
        responseType: "blob",
      });
      const url = URL.createObjectURL(res.data);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${email}.json`;
      a.click();
      URL.revokeObjectURL(url);
      showMessage("success", `已导出 ${email}.json`);
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "导出失败");
    }
  };

  const downloadAll = async () => {
    setBusy("__batch__", true);
    try {
      const res = await api.get("/creds/download-all", {
        responseType: "blob",
      });
      const url = URL.createObjectURL(res.data);
      const a = document.createElement("a");
      a.href = url;
      a.download = "antigravity_credentials.zip";
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "下载失败");
    } finally {
      setBusy("__batch__", false);
    }
  };

  const openErrors = async (email) => {
    try {
      const res = await api.get(`/creds/errors/${encodeURIComponent(email)}`);
      setErrorModal(res.data);
    } catch (err) {
      showMessage("error", err.response?.data?.detail || "获取错误信息失败");
    }
  };

  const toggleSelect = (email) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(email)) next.delete(email);
      else next.add(email);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selected.size === items.length) setSelected(new Set());
    else setSelected(new Set(items.map((i) => i.email)));
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const formatTime = (ts) => {
    if (!ts) return "-";
    return new Date(ts * 1000).toLocaleString();
  };

  const getCooldownText = (cooldowns) => {
    const active = Object.entries(cooldowns || {}).filter(
      ([, until]) => until > now
    );
    if (!active.length) return null;
    const longest = Math.max(...active.map(([, until]) => until));
    const remain = longest - now;
    const h = Math.floor(remain / 3600);
    const m = Math.floor((remain % 3600) / 60);
    const s = Math.floor(remain % 60);
    return h > 0 ? `${h}h${m}m` : m > 0 ? `${m}m${s}s` : `${s}s`;
  };

  const tierStyle = (tier) =>
    tier === "ultra" || tier === "ultra_lite"
      ? "bg-red-500/10 text-red-700 border-red-500/30"
      : tier === "pro"
      ? "bg-amber-500/10 text-amber-700 border-amber-500/30"
      : "bg-blue-500/10 text-blue-700 border-blue-500/30";

  return (
    <div className="space-y-4 sm:space-y-6">
      {/* 消息弹窗 */}
      {message && (
        <Modal
          type={message.type}
          title={message.title}
          onClose={() => setMessage(null)}
        >
          <span className="whitespace-pre-wrap">{message.text}</span>
        </Modal>
      )}

      {/* 状态统计 */}
      <div className="grid grid-cols-3 gap-2 sm:gap-4">
        <div className="bg-slate-50 border border-slate-300 rounded-xl p-3 sm:p-4 text-center">
          <div className="text-xl sm:text-2xl font-bold text-slate-900">{stats.total}</div>
          <div className="text-xs text-slate-500 mt-0.5">总计</div>
        </div>
        <div className="bg-emerald-50 border border-emerald-700/30 rounded-xl p-3 sm:p-4 text-center">
          <div className="text-xl sm:text-2xl font-bold text-emerald-700">{stats.normal}</div>
          <div className="text-xs text-slate-500 mt-0.5">正常</div>
        </div>
        <div className="bg-red-50 border border-red-700/30 rounded-xl p-3 sm:p-4 text-center">
          <div className="text-xl sm:text-2xl font-bold text-red-700">{stats.disabled}</div>
          <div className="text-xs text-slate-500 mt-0.5">禁用</div>
        </div>
      </div>

      {/* 操作按钮 */}
      <div className="flex flex-wrap items-center gap-2">
        <button
          onClick={fetchCreds}
          disabled={loading}
          className="btn btn-secondary text-xs sm:text-sm px-3 py-1.5 sm:px-4 sm:py-2 flex items-center gap-1.5"
        >
          <RefreshCw size={15} className={loading ? "animate-spin" : ""} />
          刷新状态
        </button>
        <button
          onClick={downloadAll}
          disabled={busyFiles.has("__batch__")}
          className="btn btn-secondary text-xs sm:text-sm px-3 py-1.5 sm:px-4 sm:py-2 flex items-center gap-1.5"
        >
          <Download size={15} />
          导出全部
        </button>
      </div>

      <button onClick={refreshPageQuotas} disabled={loading || busyFiles.has("__quota__")} className="btn btn-secondary text-xs flex items-center gap-1.5">
        <RefreshCw size={14} className={busyFiles.has("__quota__") ? "animate-spin" : ""} /> 刷新本页额度
      </button>

      {/* 筛选器 */}
      <div className="grid grid-cols-2 sm:flex sm:flex-wrap gap-2 sm:gap-3 text-sm">
        <Select
          value={statusFilter}
          onChange={(v) => {
            setStatusFilter(v);
            setPage(1);
          }}
          className="w-full sm:w-32"
          options={[
            { value: "all", label: "全部凭证" },
            { value: "enabled", label: "仅启用" },
            { value: "disabled", label: "仅禁用" },
          ]}
        />
        <Select
          value={errorCodeFilter}
          onChange={(v) => {
            setErrorCodeFilter(v);
            setPage(1);
          }}
          className="w-full sm:w-32"
          options={[
            { value: "all", label: "错误码: 全部" },
            { value: "none", label: "无错误" },
            { value: "400", label: "400" },
            { value: "403", label: "403" },
            { value: "404", label: "404" },
            { value: "429", label: "429" },
            { value: "500", label: "500" },
          ]}
        />
        <Select
          value={cooldownFilter}
          onChange={(v) => {
            setCooldownFilter(v);
            setPage(1);
          }}
          className="w-full sm:w-32"
          options={[
            { value: "all", label: "冷却: 全部" },
            { value: "in_cooldown", label: "CD中" },
            { value: "no_cooldown", label: "未CD" },
          ]}
        />
        <Select
          value={tierFilter}
          onChange={(v) => {
            setTierFilter(v);
            setPage(1);
          }}
          className="w-full sm:w-32"
          options={[
            { value: "all", label: "Tier: 全部" },
            { value: "free", label: "Free" },
            { value: "pro", label: "Pro" },
            { value: "ultra", label: "Ultra" },
            { value: "ultra_lite", label: "Ultra Lite" },
          ]}
        />
        <Select
          value={pageSize}
          onChange={(v) => {
            setPageSize(Number(v));
            setPage(1);
          }}
          className="w-full sm:w-32"
          options={[20, 50, 100, 200, 500, 1000].map((n) => ({
            value: n,
            label: `每页 ${n}`,
          }))}
        />
      </div>

      {/* 批量操作 */}
      <div className="card p-3 sm:p-4">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm">
          <label className="flex items-center gap-2 text-slate-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={selected.size === items.length && items.length > 0}
              onChange={toggleSelectAll}
              className="accent-purple-600 w-4 h-4"
            />
            全选本页
          </label>
          <span className="text-slate-500 text-xs">已选 {selected.size} 项</span>
          <label className="flex items-center gap-2 text-xs text-slate-600">
            测试模型
            <input aria-label="测试模型" value={testModel} onChange={e => setTestModel(e.target.value)} disabled={batchRunning} className="input w-56 text-xs" maxLength={128} />
          </label>
          <div className="flex flex-wrap gap-2">
            <button onClick={batchTest} disabled={!selected.size || !testModel.trim() || busyFiles.has("__batch__")} className="btn btn-primary text-xs px-3 py-1.5 disabled:opacity-40">批量测试</button>
            <button onClick={downloadSelected} disabled={!selected.size || busyFiles.has("__batch__")} className="btn btn-secondary text-xs px-3 py-1.5 disabled:opacity-40">导出选中</button>
            <button
              onClick={() => doBatchAction("enable")}
              disabled={selected.size === 0 || busyFiles.has("__batch__")}
              className="btn btn-secondary text-xs px-3 py-1.5 disabled:opacity-40"
            >
              批量启用
            </button>
            <button
              onClick={() => doBatchAction("disable")}
              disabled={selected.size === 0 || busyFiles.has("__batch__")}
              className="btn btn-secondary text-xs px-3 py-1.5 disabled:opacity-40"
            >
              批量禁用
            </button>
            <button
              onClick={() => doBatchAction("delete")}
              disabled={selected.size === 0 || busyFiles.has("__batch__")}
              className="btn btn-danger text-xs px-3 py-1.5 disabled:opacity-40"
            >
              批量删除
            </button>
          </div>
        </div>
      </div>

      {batchProgress && <div className="card p-4 text-sm space-y-2" aria-live="polite">
        <div className="flex flex-wrap items-center gap-3">
          <span>{batchRunning ? "测试中" : batchProgress.done < batchProgress.total ? "已停止" : "测试完成"}：{batchProgress.done}/{batchProgress.total}</span>
          <span className="text-emerald-700">成功 {batchProgress.success}</span>
          <span className="text-amber-700">限流 {batchProgress.limited}</span>
          <span className="text-red-700">失败 {batchProgress.failed}</span>
          {batchRunning && <button onClick={() => { stopBatch.current = true; }} className="btn btn-secondary text-xs">停止后续测试</button>}
        </div>
        <details><summary className="cursor-pointer text-slate-600">查看逐项结果</summary><div className="max-h-64 overflow-auto mt-2 space-y-2">
          {batchProgress.results.map(r => <div key={r.email} className="border-b border-slate-100 pb-2 text-xs break-all"><strong>{r.email}</strong> · {r.model} · HTTP {r.status || "—"}<div className={r.status === 200 ? "text-emerald-700" : "text-red-700"}>{r.message}</div></div>)}
        </div></details>
      </div>}

      {/* 凭证卡片列表 */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="animate-spin text-purple-500" size={28} />
        </div>
      ) : items.length === 0 ? (
        <div className="card text-center py-12 text-slate-500 text-sm">
          没有凭证。请先通过 OAuth 认证或批量导入添加凭证。
        </div>
      ) : (
        <div className="grid gap-3 sm:gap-4 grid-cols-1 md:grid-cols-2 xl:grid-cols-3">
          {items.map((cred) => {
            const cooldownText = getCooldownText(cred.model_cooldowns);
            const busy = busyFiles.has(cred.email);
            return (
              <div
                key={cred.email}
                className={`card p-3 sm:p-4 space-y-3 transition-colors ${
                  cred.disabled ? "border-red-500/30" : ""
                }`}
              >
                {/* 头部：选择框 + 邮箱 + 状态标签 */}
                <div className="flex items-start gap-2.5">
                  <input
                    type="checkbox"
                    checked={selected.has(cred.email)}
                    onChange={() => toggleSelect(cred.email)}
                    className="accent-purple-600 w-4 h-4 flex-shrink-0 mt-0.5"
                  />
                  <div className="flex-1 min-w-0 space-y-1.5">
                    <div className="text-sm font-medium text-slate-900 break-all leading-snug">
                      {cred.email}
                    </div>
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span
                        className={`text-xs px-2 py-0.5 rounded-full border ${tierStyle(cred.tier)}`}
                      >
                        {cred.tier === "ultra_lite" ? "Ultra Lite" : cred.tier === "ultra" ? "Ultra" : cred.tier || "pro"}
                      </span>
                      {cred.disabled ? (
                        <span className="text-red-700 text-xs flex items-center gap-1">
                          <Ban size={12} /> 禁用
                        </span>
                      ) : (
                        <span className="text-emerald-700 text-xs flex items-center gap-1">
                          <CheckCircle size={12} /> 启用
                        </span>
                      )}
                    </div>
                  </div>
                </div>

                {/* 信息区 */}
                <div className="space-y-1 text-xs">
                  <div className="flex items-center justify-between gap-2">
                    {cooldownText ? (
                      <span className="text-cyan-700 flex items-center gap-1">
                        <Snowflake size={12} /> CD {cooldownText}
                      </span>
                    ) : (
                      <span className="text-slate-500 flex items-center gap-1">
                        <Clock size={12} /> 无CD
                      </span>
                    )}
                    {cred.error_codes?.length ? (
                      <button
                        onClick={() => openErrors(cred.email)}
                        className="text-red-700 hover:text-red-700 flex items-center gap-1"
                      >
                        <AlertCircle size={12} />
                        {cred.error_codes.join(",")}
                      </button>
                    ) : (
                      <span className="text-slate-500 flex items-center gap-1">
                        <CheckCircle size={12} /> 无错误
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-1 text-slate-500">
                    <Clock size={12} className="flex-shrink-0" />
                    <span className="truncate" title={formatTime(cred.last_success)}>
                      {formatTime(cred.last_success)}
                    </span>
                  </div>
                </div>

                <div className="rounded-lg bg-slate-50 border border-slate-200 p-3">
                  <div className="flex justify-between items-center mb-2"><span className="text-xs font-medium text-slate-700">剩余额度</span><button onClick={() => requestQuota(cred.email, true)} disabled={quotaLoading.has(cred.email)} className="text-xs text-purple-700 disabled:opacity-50">刷新额度</button></div>
                  {cred.disabled && !quotaCache[cred.email] ? <p className="text-xs text-slate-500">已禁用，点击刷新可查询额度</p> : <QuotaSummary data={quotaCache[cred.email]} loading={quotaLoading.has(cred.email)} />}
                </div>
                {testResults[cred.email] && <div className={`text-xs ${testResults[cred.email].status === 200 ? "text-emerald-700" : "text-red-700"}`}>最近测试：{testResults[cred.email].model} · HTTP {testResults[cred.email].status || "—"}</div>}

                {/* 操作按钮（2×3 网格铺满卡片） */}
                <div className="grid grid-cols-3 gap-1.5 pt-1 border-t border-slate-200">
                  {busy ? (
                    <div className="col-span-3 flex items-center justify-center py-1.5">
                      <Loader2 size={16} className="animate-spin text-purple-700" />
                    </div>
                  ) : (
                    <>
                      <button
                        onClick={() => verifyProject(cred.email)}
                        className="text-xs px-2 py-1.5 rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center justify-center gap-1"
                        title="检验 Project ID / Tier（可修复 403）"
                      >
                        <Shield size={13} /> 检验
                      </button>
                      <button
                        onClick={() => testCredential(cred.email)}
                        className="text-xs px-2 py-1.5 rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center justify-center gap-1"
                        title="测试凭证"
                      >
                        <Zap size={13} /> 测试
                      </button>
                      <button
                        onClick={() => openQuotaModal(cred.email)}
                        disabled={cred.disabled}
                        className="text-xs px-2 py-1.5 rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center justify-center gap-1 disabled:opacity-40"
                        title="查看额度"
                      >
                        <BarChart3 size={13} /> 额度
                      </button>
                      <button
                        onClick={() => exportCredential(cred.email)}
                        className="text-xs px-2 py-1.5 rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center justify-center gap-1"
                        title="导出凭证（邮箱.json）"
                      >
                        <Download size={13} /> 导出
                      </button>
                      <button
                        onClick={() =>
                          doAction(cred.email, cred.disabled ? "enable" : "disable")
                        }
                        className={`text-xs px-2 py-1.5 rounded-lg flex items-center justify-center gap-1 ${
                          cred.disabled
                            ? "bg-emerald-600/20 hover:bg-emerald-600/30 text-emerald-700"
                            : "bg-amber-600/20 hover:bg-amber-600/30 text-amber-700"
                        }`}
                      >
                        {cred.disabled ? <CheckCircle size={13} /> : <Ban size={13} />}
                        {cred.disabled ? "启用" : "禁用"}
                      </button>
                      <button
                        onClick={() => doAction(cred.email, "delete")}
                        className="text-xs px-2 py-1.5 rounded-lg bg-red-600/10 hover:bg-red-600/20 text-red-700 flex items-center justify-center gap-1"
                        title="删除"
                      >
                        <Trash2 size={13} /> 删除
                      </button>
                    </>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* 分页 */}
      {total > pageSize && (
        <div className="flex items-center justify-between text-sm">
          <span className="text-slate-500 text-xs sm:text-sm">
            共 {total} 个凭证，第 {page}/{totalPages} 页
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1}
              className="btn btn-secondary text-xs px-3 py-1.5 disabled:opacity-40"
            >
              上一页
            </button>
            <span className="text-slate-600 text-xs sm:text-sm">
              {page} / {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages}
              className="btn btn-secondary text-xs px-3 py-1.5 disabled:opacity-40"
            >
              下一页
            </button>
          </div>
        </div>
      )}


      {/* 额度弹窗 */}
      {quotaModal && (
        <Modal
          type="info"
          title={`额度信息 - ${quotaModal.email}`}
          onClose={() => setQuotaModal(null)}
        >
          <QuotaSummary data={quotaCache[quotaModal.email]} loading={quotaLoading.has(quotaModal.email)} />
        </Modal>
      )}

      {/* 错误详情弹窗 */}
      {errorModal && (
        <div
          className="fixed inset-0 bg-slate-900/30 flex items-center justify-center p-4 z-50"
          onClick={() => setErrorModal(null)}
        >
          <div
            className="bg-white border border-slate-300 rounded-xl max-w-2xl w-full max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-4 sm:px-5 py-4 border-b border-slate-200">
              <h3 className="font-semibold flex items-center gap-2 text-sm sm:text-base break-all">
                <AlertCircle size={16} className="text-red-700 flex-shrink-0" />
                错误详情 - {errorModal.email}
              </h3>
              <button
                onClick={() => setErrorModal(null)}
                className="text-slate-600 hover:text-slate-900 flex-shrink-0"
              >
                ✕
              </button>
            </div>
            <div className="p-4 sm:p-5 overflow-y-auto space-y-4">
              <div>
                <div className="text-sm text-slate-600 mb-1">错误码</div>
                <div className="text-red-700 text-sm">
                  {(errorModal.error_codes || []).join(", ") || "无"}
                </div>
              </div>
              <div>
                <div className="text-sm text-slate-600 mb-1">错误信息</div>
                <pre className="bg-slate-50 border border-slate-300 rounded-lg p-3 sm:p-4 text-xs text-slate-700 overflow-auto max-h-56 whitespace-pre-wrap">
                  {JSON.stringify(errorModal.error_messages, null, 2)}
                </pre>
              </div>
              <div className="flex flex-wrap gap-2">
                <button
                  onClick={() => {
                    verifyProject(errorModal.email);
                    setErrorModal(null);
                  }}
                  className="btn btn-primary text-sm"
                >
                  检验修复（重新获取Project ID）
                </button>
                <button
                  onClick={() => {
                    testCredential(errorModal.email);
                    setErrorModal(null);
                  }}
                  className="btn btn-secondary text-sm"
                >
                  测试凭证
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
