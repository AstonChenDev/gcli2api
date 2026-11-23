import { useEffect } from "react";
import { AlertCircle, CheckCircle, Info, X } from "lucide-react";

/**
 * 通用消息弹窗
 * type: success / error / info
 */
export default function Modal({ type = "info", title, children, onClose }) {
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") onClose?.();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const styles = {
    success: { icon: <CheckCircle size={20} className="text-green-700" />, border: "border-green-500/40", iconBg: "bg-green-500/15" },
    error: { icon: <AlertCircle size={20} className="text-red-700" />, border: "border-red-500/40", iconBg: "bg-red-500/15" },
    info: { icon: <Info size={20} className="text-blue-700" />, border: "border-blue-500/40", iconBg: "bg-blue-500/15" },
  }[type] || {};

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/30" onClick={onClose}>
      <div
        className={`card max-w-md w-full border ${styles.border} p-0 overflow-hidden`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200">
          <div className="flex items-center gap-2">
            <span className={`p-1.5 rounded-lg ${styles.iconBg}`}>{styles.icon}</span>
            <h3 className="text-sm font-semibold text-slate-800">{title}</h3>
          </div>
          <button onClick={onClose} className="text-slate-600 hover:text-slate-900">
            <X size={16} />
          </button>
        </div>
        <div className="px-4 py-3 text-sm text-slate-700 max-h-[60vh] overflow-y-auto break-all">
          {children}
        </div>
        <div className="px-4 py-3 border-t border-slate-200 text-right">
          <button onClick={onClose} className="btn btn-secondary text-xs px-4 py-1.5">
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}
