import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import QuotaSummary from "./QuotaSummary";

export default function CredentialQuota({ email, cooling, disabled, data, loading, onRequest }) {
  const [expanded, setExpanded] = useState(false);
  const visible = !cooling || expanded;

  const toggle = () => {
    setExpanded(!expanded);
    if (!expanded) onRequest(email, true);
  };

  return <section aria-label={`${email} 剩余额度`} className="rounded-lg bg-slate-50 border border-slate-200 p-3">
    <div className={`flex justify-between items-center gap-2 ${visible ? "mb-2" : ""}`}>
      <span className="text-xs font-medium text-slate-700">剩余额度</span>
      <div className="flex items-center gap-3">
        {visible && <button onClick={() => onRequest(email, true)} disabled={loading} className="text-xs text-purple-700 disabled:opacity-50">刷新额度</button>}
        {cooling && <button onClick={toggle} aria-expanded={visible} className="text-xs text-purple-700 flex items-center gap-1">
          {visible ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
          {visible ? "收起额度" : "CD 中 · 展开额度"}
        </button>}
      </div>
    </div>
    {visible && (disabled && !data
      ? <p className="text-xs text-slate-500">已禁用，点击刷新可查询额度</p>
      : <QuotaSummary data={data} loading={loading} />)}
  </section>;
}
