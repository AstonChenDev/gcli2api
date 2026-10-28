import api from "../api";

// Stream results as each credential finishes; no two-minute Axios request timeout.
export async function uploadCredentialFiles(formData, { signal, onEvent }) {
  const token = localStorage.getItem("agy2apiPreviewPanelToken");
  const response = await fetch(`${api.defaults.baseURL || ""}/creds/upload-stream`, {
    method: "POST", body: formData, signal,
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(typeof error.detail === "string" ? error.detail : `导入请求失败（HTTP ${response.status}）`);
  }
  if (!response.body) throw new Error("浏览器不支持读取导入进度，请更新浏览器后重试");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = "", complete = false;
  const consume = line => {
    if (!line.trim()) return;
    const event = JSON.parse(line);
    if (event.type === "complete") complete = true;
    onEvent(event);
  };
  try {
    while (true) {
      const { done, value } = await reader.read();
      pending += decoder.decode(value, { stream: !done });
      const lines = pending.split("\n"); pending = lines.pop();
      lines.forEach(consume);
      if (done) { consume(pending); break; }
    }
    if (!complete) throw new Error("导入连接中断。已保存的凭证会保留，请核对进度后重试未完成项");
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
