import { FileJson, Loader2, Upload, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { uploadCredentialFiles } from "../utils/credentialUpload";

export default function UploadPage() {
  const [files, setFiles] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [results, setResults] = useState(null);
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef(null);
  const controllerRef = useRef(null);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; controllerRef.current?.abort(); };
  }, []);

  const addFiles = (fileList) => {
    if (uploading) return;
    const valid = Array.from(fileList).filter(
      (f) => f.name.toLowerCase().endsWith(".json") || f.name.toLowerCase().endsWith(".zip")
    );
    setFiles((prev) => {
      const names = new Set(prev.map((f) => f.name));
      return [...prev, ...valid.filter((f) => !names.has(f.name))];
    });
    setResults(null);
  };

  const removeFile = (name) => {
    setFiles((prev) => prev.filter((f) => f.name !== name));
  };

  const upload = async () => {
    if (files.length === 0 || uploading) return;
    const controller = new AbortController();
    controllerRef.current = controller;
    setUploading(true);
    setResults(null);
    try {
      const formData = new FormData();
      files.forEach(f => formData.append("files", f));
      await uploadCredentialFiles(formData, {
        signal: controller.signal,
        onEvent: event => {
          if (!mounted.current || event.type === "heartbeat") return;
          if (event.type === "start") setResults({ ...event, results: [] });
          if (event.type === "progress") setResults(previous => ({ ...event, results: [...(previous?.results || []), event.result] }));
          if (event.type === "complete") setResults(event);
        },
      });
    } catch (err) {
      if (mounted.current) setResults(previous => ({ ...previous,
        error: controller.signal.aborted ? "已停止导入。已保存的凭证保留，未完成项可重新导入。" : (err.message || "导入连接失败，请检查网络后重试"),
      }));
    } finally {
      if (mounted.current) setUploading(false);
      if (controllerRef.current === controller) controllerRef.current = null;
    }
  };

  const successCount = results?.uploaded_count || 0;
  const totalCount = results?.total_count || 0;

  return (
    <div className="space-y-6">
      <div className="bg-gradient-to-br from-emerald-50 to-white border border-emerald-700/30 rounded-xl p-5">
        <h3 className="text-lg font-semibold text-emerald-700 mb-2">批量导入凭证</h3>
        <p className="text-sm text-slate-600">
          支持批量导入 Antigravity 凭证（.json 或包含 JSON 文件的 .zip）。
        </p>
        <div className="mt-3 text-xs text-slate-500 bg-slate-50/60 rounded-lg p-3">
          <p className="mb-1 font-medium text-slate-600">规范 JSON 格式：</p>
          <pre className="text-emerald-700/80 whitespace-pre-wrap font-mono">{`{
  "refresh_token": "必填",
  "email": "可选，缺失自动获取",
  "tier": "可选，缺失自动获取",
  "project_id": "可选，缺失自动获取"
}`}</pre>
          <p className="mt-2">每次最多同时验证 3 个凭证，逐项显示结果；同邮箱凭证自动去重（覆盖更新）。网络失败会自动重试一次。</p>
        </div>
      </div>

      {/* 上传区域 */}
      <div
        className={`border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-colors ${
          dragOver
            ? "border-purple-500 bg-purple-500/10"
            : "border-slate-300 hover:border-slate-400"
        }`}
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragOver(false);
          addFiles(e.dataTransfer.files);
        }}
      >
        <Upload className="mx-auto text-purple-700 mb-3" size={32} />
        <p className="text-slate-700">点击选择文件或拖拽文件到此区域</p>
        <p className="text-slate-500 text-sm mt-1">支持 .json 和 .zip 格式，ZIP 会自动提取其中的 JSON 凭证</p>
        <input
          ref={inputRef}
          type="file"
          multiple
          accept=".json,.zip"
          className="hidden"
          onChange={(e) => {
            addFiles(e.target.files);
            e.target.value = "";
          }}
        />
      </div>

      {/* 文件列表 */}
      {files.length > 0 && (
        <div className="card space-y-3">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium text-slate-700">
              已选择 {files.length} 个文件
            </h4>
            <div className="flex gap-2">
              <button
                onClick={upload}
                disabled={uploading}
                className="btn btn-primary text-sm flex items-center gap-1.5 disabled:opacity-50"
              >
                {uploading ? (
                  <Loader2 size={14} className="animate-spin" />
                ) : (
                  <Upload size={14} />
                )}
                {uploading ? "上传中..." : "上传文件"}
              </button>
              <button
                onClick={() => setFiles([])}
                disabled={uploading}
                className="btn btn-secondary text-sm"
              >
                清空列表
              </button>
            </div>
          </div>
          <div className="space-y-1.5 max-h-64 overflow-y-auto">
            {files.map((f) => (
              <div
                key={f.name}
                className="flex items-center justify-between bg-slate-50 border border-slate-300 rounded-lg px-3 py-2 text-sm"
              >
                <div className="flex items-center gap-2 text-slate-700 min-w-0">
                  <FileJson size={15} className="text-purple-700 flex-shrink-0" />
                  <span className="truncate">{f.name}</span>
                </div>
                <div className="flex items-center gap-3 flex-shrink-0">
                  <span className="text-slate-500 text-xs">
                    {(f.size / 1024).toFixed(1)} KB
                  </span>
                  <button
                    disabled={uploading}
                    onClick={() => removeFile(f.name)}
                    className="text-slate-500 hover:text-red-700"
                  >
                    <X size={14} />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {uploading && <div className="card space-y-3" role="status">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm">{results ? `正在验证：${results.completed_count || 0} / ${results.total_count}，成功 ${results.uploaded_count || 0}，失败 ${(results.completed_count || 0) - (results.uploaded_count || 0)}` : "正在上传并读取文件…"}</p>
          <button onClick={() => controllerRef.current?.abort()} className="btn btn-secondary text-sm">停止导入</button>
        </div>
        {results && <progress aria-label="凭证导入进度" max={results.total_count || 1} value={results.completed_count || 0} className="w-full accent-purple-600" />}
        <p className="text-xs text-slate-500">单个凭证验证最多 90 秒；离开此页面会停止未完成的导入，已成功保存的凭证保留。</p>
      </div>}
      {/* 上传结果 */}
      {results && <div className="card space-y-3">
        {results.error && <p role="alert" className="bg-amber-50 border border-amber-300 text-amber-800 px-4 py-3 rounded-lg">{results.error}</p>}
        {!uploading && <p className="text-sm text-slate-700">{results.message || `已处理 ${results.completed_count || 0}/${totalCount}，成功保存 ${successCount} 个凭证`}</p>}
        {results.results?.length > 0 && <div className="space-y-1.5 max-h-96 overflow-y-auto">
          {results.results.map((row, index) => <div key={row.item_index ?? index} className={`text-xs px-3 py-2 rounded-lg border break-words ${row.status === "success" ? "bg-green-50 border-green-200 text-green-800" : "bg-red-50 border-red-200 text-red-800"}`}>
            <span className="font-medium">{row.status === "success" ? "✓" : "✗"} {row.filename}</span>：{row.message}
          </div>)}
        </div>}
      </div>}

    </div>
  );
}
