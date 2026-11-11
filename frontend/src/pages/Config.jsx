import { Loader2, Plus, RefreshCw, Save, Trash2 } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import Modal from "../components/Modal";
import Select from "../components/Select";
import api from "../api";

// 错误码输入：编辑期间保留原始文本，仅在按键时把已解析的数组回写配置（保证保存时取到最新值），
// 失焦时再归一化显示。原实现在每次按键即时解析并回写数组，导致分隔符与未成形的中间输入被即时
// 过滤/截断，无法输入多个错误码，也无法输入小于 100 的中间数字。
function CsvNumbersField({ value, onChange, placeholder, className }) {
  const toText = (v) => (Array.isArray(v) ? v.join(", ") : (v ?? ""));
  const [text, setText] = useState(() => toText(value));
  const focusedRef = useRef(false);

  // 仅在非编辑态（失焦）时同步外部配置变化，避免编辑过程中被即时覆盖
  useEffect(() => {
    if (!focusedRef.current) setText(toText(value));
  }, [value]);

  const commitText = (raw) => {
    const nums = raw
      .split(/[,\s]+/)
      .map((x) => parseInt(x, 10))
      .filter((n) => Number.isInteger(n) && n >= 100 && n <= 599);
    onChange([...new Set(nums)].sort((a, b) => a - b));
  };

  return (
    <input
      type="text"
      value={text}
      onFocus={() => {
        focusedRef.current = true;
      }}
      onChange={(e) => {
        const raw = e.target.value;
        setText(raw);
        commitText(raw);
      }}
      onBlur={() => {
        focusedRef.current = false;
        commitText(text);
      }}
      placeholder={placeholder}
      className={className}
    />
  );
}

// 多行文本（敏感词 / IP 白名单等，每行一条）：编辑期间保留原始文本（允许末尾空行与连续回车），
// 仅在按键时把清洗后的数组回写配置（保证保存时取到最新值），失焦时再归一化显示。
// 原实现在每次按键即时 split + 过滤空行，末尾按回车产生的新空行被立即移除，
// 导致"最后一行按回车没反应"、无法换行新增词条。
function LinesField({ value, onChange, placeholder, className, rows = 6 }) {
  const toText = (v) => (Array.isArray(v) ? v.join("\n") : v ?? "");
  const [text, setText] = useState(() => toText(value));
  const focusedRef = useRef(false);

  // 仅在非编辑态（失焦）时同步外部配置变化，避免编辑过程中被即时覆盖
  useEffect(() => {
    if (!focusedRef.current) setText(toText(value));
  }, [value]);

  const commitText = (raw) => {
    const lines = raw
      .split(/\r?\n/)
      .map((w) => w.replace(/\s+$/, ""))
      .filter((w) => w.trim() !== "");
    onChange(lines);
  };

  return (
    <textarea
      value={text}
      onFocus={() => {
        focusedRef.current = true;
      }}
      onChange={(e) => {
        const raw = e.target.value;
        setText(raw);
        commitText(raw);
      }}
      onBlur={() => {
        focusedRef.current = false;
        commitText(text);
      }}
      placeholder={placeholder}
      rows={rows}
      className={className}
    />
  );
}

const CONFIG_GROUPS = [
  {
    title: "499 等待异常保护",
    fields: [
      { key: "cooldown_499_mode", label: "运行模式", type: "select", options: [
        { value: "off", label: "关闭" }, { value: "observe", label: "仅观察（默认）" },
        { value: "enforce", label: "执行冷却" },
      ], note: "499 不一定是限流。仅统计等待超过下方时间、上游没有任何事件、下游没有输出的客户端断开。仅观察记录触发情况，不限制派发。" },
      { key: "cooldown_499_threshold", label: "连续异常触发次数", type: "number", min: 1, max: 100, step: 1, note: "1–100 次；窗口内符合条件的异常累计，完整成功会清零。同一请求、同一账号只计一次。" },
      { key: "cooldown_499_window_seconds", label: "统计窗口（秒）", type: "number", min: 1, max: 86400, step: 1, note: "1–86400 秒，默认 300 秒。" },
      { key: "cooldown_499_min_wait_seconds", label: "最低等待时间（秒）", type: "number", min: 0, max: 3600, step: 1, note: "0–3600 秒，默认 90 秒。低于此值的客户端断开不触发保护；保存后用于新派发请求。" },
      { key: "cooldown_499_duration_seconds", label: "闲置冷却时长（秒）", type: "number", min: 1, max: 86400, step: 1, note: "1–86400 秒，默认 1200 秒。触发后停止新派发，保留在途请求；全部结束后开始计时。已触发的冷却保留原时长。20 分钟是否恢复仍需实测。" },
      { key: "cooldown_499_scope", label: "冷却范围", type: "select", options: [
        { value: "account", label: "整个账号（默认）" }, { value: "family", label: "当前模型家族" },
      ], note: "账号范围覆盖本服务的全部生成请求；模型家族范围只限制同一家族。改变模式或范围会清空保护历史并重新统计，已在途请求继续完成。" },
      { key: "cooldown_499_custom_families_enabled", label: "启用自定义模型家族", type: "bool", showIf: cfg => cfg.cooldown_499_scope === "family", note: "关闭时沿用默认分组：Argon、其他 Gemini、Claude/GPT。开启后按下方规则划分，仅影响 499 保护。修改分组开关、规则或未匹配处理会重新统计保护历史，在途请求继续完成。" },
      { key: "cooldown_499_family_fallback", label: "未匹配模型的处理", type: "select", showIf: cfg => cfg.cooldown_499_scope === "family" && cfg.cooldown_499_custom_families_enabled, options: [
        { value: "default", label: "沿用默认家族" }, { value: "model", label: "每个模型单独冷却" },
      ], note: "自定义组独立于默认家族，即使同名也不会合并。想让所有 Argon 变体共用一组，可填写 *argon*。" },
      { key: "cooldown_499_family_rules", label: "自定义分组规则", type: "families", showIf: cfg => cfg.cooldown_499_scope === "family" && cfg.cooldown_499_custom_families_enabled, note: "最多 32 组，每组最多 32 条匹配规则。模型名不区分大小写，支持完整名称、*（任意字符）和 ?（一个字符）。多组命中时，以排在最前面的组为准；更具体的规则应放在前面。" },
      { key: "cooldown_499_probe_concurrency", label: "恢复试探并发数", type: "number", min: 1, max: 32, step: 1, note: "1–32，默认 1。冷却结束后允许有限的自然请求试探；完整成功恢复派发，符合条件的异常再次冷却。全部账号不可用时返回 503 和 Retry-After。设置保存后对所有共享同一数据库的进程生效。" },
    ],
  },
  {
    title: "服务器配置",
    fields: [
      { key: "host", label: "服务器主机地址", type: "text", note: "服务器监听的主机地址，0.0.0.0表示监听所有接口（重启生效）" },
      { key: "port", label: "服务器端口", type: "number", note: "服务器监听的端口号（重启生效）" },
      { key: "api_password", label: "API 访问密码", type: "text", note: "聊天API访问密码，用于 OpenAI / Gemini / Claude 端点认证" },
      { key: "panel_password", label: "控制面板密码", type: "text", note: "控制面板访问密码" },
    ],
  },
  {
    title: "IP 白名单",
    fields: [
      { key: "ip_whitelist_enabled", label: "启用 IP 白名单", type: "bool", note: "开启后仅白名单内的 IP 可访问面板和 API，其余 IP 直接 403。留空列表或关闭开关则不限制。保存后立即生效（热更新）" },
      { key: "ip_whitelist", label: "IP 白名单列表", type: "textarea", showIf: (cfg) => !!cfg.ip_whitelist_enabled, note: "每行一条，支持单个 IP、CIDR 网段、起止范围（- 或 ~ 分隔），IPv4 / IPv6 均可", placeholder: "例如：\n127.0.0.1\n192.168.1.0/24\n10.0.0.1-10.0.0.100\n::1\n2001:db8::/32" },
    ],
  },
  {
    title: "基础配置",
    fields: [
      { key: "credentials_dir", label: "凭证目录路径", type: "text", note: "存储认证文件的目录路径" },
      { key: "proxy", label: "代理设置", type: "text", note: "支持 http/https/socks5/socks5h，可带账号密码（user:pass@host:port），留空不使用。保存后对新连接生效", placeholder: "http://user:pass@proxy:port 或 socks5h://proxy:port" },
      { key: "stream_handshake_timeout_enabled", label: "启用握手超时", type: "bool", note: "限制流式调用连接上游的等待时间，使用代理和不使用代理时均生效。默认开启；关闭后不限建连时间，客户端断开仍会取消请求。保存后对新请求生效" },
      { key: "stream_handshake_timeout_seconds", label: "握手超时秒数", type: "number", min: 1, max: 3600, step: 1, showIf: (cfg) => !!cfg.stream_handshake_timeout_enabled, note: "默认 40 秒，可设 1–3600 秒。每次重试单独计时，不限制连接建立后的思考与生成时长" },
    ],
  },
  {
    title: "端点配置",
    fields: [
      { key: "oauth_proxy_url", label: "OAuth Endpoint", type: "text", note: "Google OAuth2 端点，用于 token 获取和刷新" },
      { key: "googleapis_proxy_url", label: "Google APIs Endpoint", type: "text", note: "Google APIs 端点" },
      { key: "antigravity_api_url", label: "Antigravity API Endpoint", type: "text", note: "Antigravity API 端点" },
    ],
  },
  {
    title: "自动封禁配置",
    fields: [
      { key: "auto_ban_enabled", label: "启用自动封禁", type: "bool", note: "遇到指定错误码时自动禁用凭证" },
      { key: "auto_ban_error_codes", label: "自动封禁错误码", type: "text", note: "JSON 数组格式，如 [403, 400]" },
    ],
  },
  {
    title: "重试配置",
    fields: [
      { key: "retry_429_enabled", label: "启用错误重试", type: "bool", note: "遇到错误时自动重试" },
      { key: "retry_429_max_retries", label: "最大重试次数", type: "number", note: "遇到错误时的最大重试次数" },
      { key: "retry_429_interval", label: "重试间隔（秒）", type: "number", note: "两次重试间的等待秒数，允许 0（立即重试），范围 0-3600" },
    ],
  },
  {
    title: "凭证选择模式",
    fields: [
      {
        key: "credential_select_mode",
        label: "选择模式",
        type: "select",
        note: "决定每次 API 请求如何挑选凭证",
        options: [
          { value: "random", label: "完全随机" },
          { value: "cache", label: "缓存匹配（推荐）" },
          { value: "sticky", label: "凭证不变直到 CD" },
        ],
        descriptions: {
          random: "完全随机负载均衡：每次请求从可用凭证中随机挑选一个，最均匀地分摊负载，但相同会话可能命中不同凭证，无法利用上游 prompt cache。",
          cache: "缓存匹配（默认推荐）：以\"模型 + system 指令（或首条 user 消息）\"哈希出会话 key，相同会话始终复用同一凭证——命中 Google 上游的 prompt cache，长对话更快、更省 token。凭证进入 CD 后自动切换并重新绑定。建议搭配 Redis（REDIS_URL）实现多实例共享。",
          sticky: "凭证粘性：所有请求锁定同一个凭证，直到该凭证进入冷却（CD）或被禁用才切换到下一个。适合最大化单凭证的缓存命中，但负载不均——单凭证扛满全量请求，其余凭证闲置。",
        },
      },
    ],
  },
  {
    title: "响应配置",
    fields: [
      { key: "return_thoughts_to_frontend", label: "返回思维链到前端", type: "bool", note: "向上游请求返回思考内容；关闭时请求上游省略。如果上游仍返回思考字段，按原版规则转发" },
    ],
  },
  {
    title: "抗截断配置",
    fields: [
      { key: "anti_truncation_max_attempts", label: "抗截断最大重试次数", type: "number", note: "检测到输出截断时的最大续传尝试次数（1-10）。使用'抗截断-'前缀的模型生效" },
    ],
  },
  {
    title: "敏感词过滤",
    fields: [
      { key: "sensitive_words_enabled", label: "启用敏感词过滤", type: "bool", note: "请求中命中敏感词的内容会被替换为空，避免触发 Google API 端点的敏感词拦截" },
      { key: "sensitive_words", label: "敏感词列表", type: "textarea", showIf: (cfg) => !!cfg.sensitive_words_enabled, note: "每行一个词或短语，不区分大小写，支持中文。命中后整词替换为空。保存后立即生效（热更新）", placeholder: "例如：\nsensitive word\n敏感词\n另一个词" },
    ],
  },
  {
    title: "思考等级模型匹配",
    fields: [
      { key: "thinking_model_routing_enabled", label: "按思考等级匹配模型", type: "bool", note: "默认关闭。开启后，例如 gemini-4-argon + high 自动选择 gemini-4-argon-high。支持 high / medium / low；已带等级后缀优先，未传等级默认选择 medium。保存后立即生效。" },
      { key: "thinking_model_routing_bases", label: "适用模型", type: "textarea", showIf: (cfg) => !!cfg.thinking_model_routing_enabled, note: "每行一个基础模型名，默认 gemini-4-argon。仅添加上游确实提供 -high / -medium / -low 变体的模型。OpenAI 使用 reasoning_effort，Gemini 使用 generationConfig.thinkingConfig.thinkingLevel。后续模型名复写规则仍可覆盖匹配结果。" },
    ],
  },
  {
    title: "模型名复写",
    fields: [
      { key: "model_rewrite_enabled", label: "启用模型名复写", type: "bool", note: "开启后按规则匹配请求中的模型名（支持正则），命中则替换为规则指定的模型名。常用于把客户端固定模型名映射到实际可用的上游模型。保存后立即生效（热更新）" },
      {
        key: "model_rewrite_rules",
        label: "复写规则",
        type: "rules",
        showIf: (cfg) => !!cfg.model_rewrite_enabled,
        note: "每行一条规则：匹配正则 + 替换模型名。自上而下首条命中即生效，替换支持正则反向引用（如 \\1）",
      },
    ],
  },
  {
    title: "请求日志",
    fields: [
      { key: "usage_log_enabled", label: "启用请求日志", type: "bool", note: "记录每次 API 请求（模型、凭证、token 用量、耗时、状态）到数据库，在\"请求日志\"页面查看。日志不自动清理，可在该页面手动清理或清空" },
      { key: "request_log_enabled", label: "启用请求体详细日志", type: "bool", note: "保存发往上游的请求头（敏感头已脱敏）和完整请求体，供按 ID 查询。清理请求日志时同步删除详细 JSON 文件" },
      { key: "request_log_all_requests", label: "保存全部新请求体", type: "bool", showIf: (cfg) => !!cfg.request_log_enabled, note: "包括成功和失败的模型调用，每次上游重试分别保存；需要同时开启请求日志。历史未保存的内容无法补回" },
      { key: "request_log_error_codes", label: "详细日志错误码", type: "csv_numbers", showIf: (cfg) => !!cfg.request_log_enabled && !cfg.request_log_all_requests, note: "仅当最终响应状态码命中此列表时记录详细日志，逗号分隔，如 400, 429。留空默认只记录 400", placeholder: "400, 429" },
    ],
  },
];

export default function Config() {
  const [config, setConfig] = useState({});
  const [envLocked, setEnvLocked] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);
  const [cooldownStatus, setCooldownStatus] = useState(null);
  const [familyModel, setFamilyModel] = useState("");
  const [familyPreview, setFamilyPreview] = useState(null);
  const [familyPreviewBusy, setFamilyPreviewBusy] = useState(false);
  const familyDraftVersion = useRef(0);

  const clearFamilyPreview = () => {
    familyDraftVersion.current += 1;
    setFamilyPreview(null);
  };

  const preparedConfig = () => ({
    ...config,
    cooldown_499_family_rules: (config.cooldown_499_family_rules || []).map(rule => ({
      name: rule.name.trim(), patterns: rule.patterns.map(p => p.trim()).filter(Boolean),
    })),
  });

  const previewFamily = async () => {
    const version = familyDraftVersion.current;
    setFamilyPreviewBusy(true);
    try {
      const draft = Object.fromEntries(Object.entries(preparedConfig()).filter(([key]) => key.startsWith("cooldown_499_")));
      const res = await api.post("/config/cooldown499/resolve", { model: familyModel, config: draft });
      if (version === familyDraftVersion.current) setFamilyPreview({ text: `匹配结果：${res.data.label}（按当前编辑内容，尚未保存的修改也会参与预览）` });
    } catch (err) {
      if (version === familyDraftVersion.current) setFamilyPreview({ error: true, text: err.response?.data?.detail || "无法预览分组" });
    } finally {
      setFamilyPreviewBusy(false);
    }
  };

  const loadCooldownStatus = async () => {
    try {
      const res = await api.get("/config/cooldown499/status");
      setCooldownStatus(res.data);
    } catch {
      setCooldownStatus(null);
    }
  };

  useEffect(() => {
    loadConfig();
  }, []);

  const loadConfig = async ({ silent = false } = {}) => {
    setLoading(true);
    try {
      const res = await api.get("/config/get");
      setConfig(res.data.config || {});
      clearFamilyPreview();
      setEnvLocked(res.data.env_locked || []);
      await loadCooldownStatus();
      if (!silent) setMessage(null);
    } catch (err) {
      setMessage({ type: "error", text: err.response?.data?.detail || "加载配置失败" });
    } finally {
      setLoading(false);
    }
  };

  const saveConfig = async () => {
    setSaving(true);
    try {
      const res = await api.post("/config/save", { config: preparedConfig() });
      setMessage({ type: "success", text: res.data.message || "配置保存成功" });
      // 静默刷新配置（不清空刚显示的弹窗消息）
      await loadConfig({ silent: true });
    } catch (err) {
      setMessage({ type: "error", text: err.response?.data?.detail || "保存配置失败" });
    } finally {
      setSaving(false);
    }
  };

  const setField = (key, value) => {
    if (key.startsWith("cooldown_499_")) clearFamilyPreview();
    setConfig((prev) => ({ ...prev, [key]: value }));
  };

  // 一键镜像/官方端点（墙内访问 Google 官方端点的反代方案）
  const MIRROR_BASE = "https://gcli-api.sukaka.top";
  const OFFICIAL_ENDPOINTS = {
    oauth_proxy_url: "https://oauth2.googleapis.com",
    googleapis_proxy_url: "https://www.googleapis.com",
    antigravity_api_url: "https://daily-cloudcode-pa.googleapis.com",
  };
  const MIRROR_PATHS = {
    oauth_proxy_url: "/oauth2",
    googleapis_proxy_url: "/googleapis",
    antigravity_api_url: "/daily-cloudcode-pa",
  };

  const useMirrorUrls = () => {
    setConfig((prev) => {
      const next = { ...prev };
      for (const [key, path] of Object.entries(MIRROR_PATHS)) {
        if (!envLocked.includes(key)) next[key] = MIRROR_BASE + path;
      }
      return next;
    });
  };

  const restoreOfficialUrls = () => {
    setConfig((prev) => {
      const next = { ...prev };
      for (const [key, url] of Object.entries(OFFICIAL_ENDPOINTS)) {
        if (!envLocked.includes(key)) next[key] = url;
      }
      return next;
    });
  };

  const renderField = (field) => {
    const isLocked = envLocked.includes(field.key);
    const value = config[field.key] ?? "";

    if (isLocked) {
      return (
        <div className="text-xs text-amber-700/80 bg-amber-500/10 border border-amber-500/20 rounded-lg px-3 py-2">
          该配置已被环境变量锁定，修改环境变量后重启生效
        </div>
      );
    }

    if (field.type === "bool") {
      return (
        <label className="flex items-center gap-3 cursor-pointer select-none">
          <button
            type="button"
            aria-label={field.label}
            role="switch"
            aria-checked={!!value}
            onClick={() => setField(field.key, !value)}
            className={`relative w-11 h-6 rounded-full transition-colors ${
              value ? "bg-purple-600" : "bg-slate-200"
            }`}
          >
            <span
              className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${
                value ? "translate-x-5" : ""
              }`}
            />
          </button>
          <span className="text-sm text-slate-700">{value ? "已启用" : "已禁用"}</span>
        </label>
      );
    }

    if (field.type === "select") {
      const current = field.options.find((o) => o.value === value) || field.options[0];
      return (
        <div className="space-y-2">
          <Select
            value={current?.value ?? ""}
            onChange={(v) => setField(field.key, v)}
            options={field.options}
            className="w-full sm:w-64"
          />
          {field.descriptions && current && (
            <div className="bg-slate-50/60 border border-slate-300 rounded-lg p-3 text-xs sm:text-sm text-slate-700 leading-relaxed">
              {field.descriptions[current.value]}
            </div>
          )}
        </div>
      );
    }

    if (field.type === "csv_numbers") {
      return (
        <CsvNumbersField
          value={value}
          onChange={(v) => setField(field.key, v)}
          placeholder={field.placeholder}
          className="w-full sm:w-64 px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-900 placeholder-slate-400 font-mono"
        />
      );
    }

    if (field.type === "families") {
      const groups = Array.isArray(value) ? value : [];
      const updateGroup = (index, key, next) => setField(field.key, groups.map((group, i) => i === index ? { ...group, [key]: next } : group));
      const moveGroup = (index, offset) => {
        const next = [...groups];
        [next[index], next[index + offset]] = [next[index + offset], next[index]];
        setField(field.key, next);
      };
      return (
        <div className="space-y-3">
          {groups.length === 0 && <p className="text-sm text-slate-500">尚未添加分组，所有模型使用“未匹配模型的处理”。</p>}
          {groups.map((group, index) => (
            <div key={index} className="border border-slate-300 rounded-lg p-3 space-y-2">
              <div className="flex flex-wrap gap-2 items-center">
                <span className="text-sm text-slate-500">第 {index + 1} 组</span>
                <input aria-label={`第 ${index + 1} 组名称`} value={group.name} maxLength={64}
                  onChange={e => updateGroup(index, "name", e.target.value)} placeholder="分组名称，如 Argon 高档"
                  className="flex-1 min-w-40 px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm" />
                <button type="button" aria-label={`上移第 ${index + 1} 组`} disabled={index === 0} onClick={() => moveGroup(index, -1)} className="btn btn-secondary text-xs disabled:opacity-40">上移</button>
                <button type="button" aria-label={`下移第 ${index + 1} 组`} disabled={index === groups.length - 1} onClick={() => moveGroup(index, 1)} className="btn btn-secondary text-xs disabled:opacity-40">下移</button>
                <button type="button" aria-label={`删除第 ${index + 1} 组`} onClick={() => setField(field.key, groups.filter((_, i) => i !== index))} className="btn btn-secondary text-xs">删除</button>
              </div>
              <textarea aria-label={`第 ${index + 1} 组模型规则`} rows={3} value={group.patterns.join("\n")}
                onChange={e => updateGroup(index, "patterns", e.target.value.split("\n"))}
                placeholder={"每行一条，例如：\ngemini-4-argon-high\ngemini-4-argon-high-*"}
                className="w-full px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm font-mono" />
            </div>
          ))}
          <button type="button" disabled={groups.length >= 32} onClick={() => setField(field.key, [...groups, { name: "", patterns: [] }])} className="btn btn-secondary text-xs disabled:opacity-40">添加模型家族</button>
          <div className="border-t border-slate-200 pt-3 space-y-2">
            <div className="flex flex-wrap gap-2">
              <input aria-label="预览分组的模型名" value={familyModel} maxLength={128} onChange={e => { setFamilyModel(e.target.value); clearFamilyPreview(); }}
                placeholder="输入实际调用的模型名，检查归属" className="flex-1 min-w-40 px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm" />
              <button type="button" disabled={familyPreviewBusy || !familyModel.trim()} onClick={previewFamily} className="btn btn-secondary text-xs disabled:opacity-40">{familyPreviewBusy ? "检查中…" : "预览匹配"}</button>
            </div>
            <p className="text-xs text-slate-500">预览仅检查分组，不保存设置，也不发送生成请求。模型名应使用思考等级匹配、模型名复写后的实际名称。</p>
            {familyPreview && <p role="status" className={`text-sm ${familyPreview.error ? "text-red-700" : "text-green-700"}`}>{familyPreview.text}</p>}
          </div>
        </div>
      );
    }

    if (field.type === "rules") {
      const rules = Array.isArray(value)
        ? value.map((r) => (Array.isArray(r) ? [String(r[0] ?? ""), String(r[1] ?? "")] : ["", ""]))
        : [];
      const updateRule = (idx, col, val) => {
        const next = rules.map((r) => [...r]);
        next[idx][col] = val;
        setField(field.key, next);
      };
      const removeRule = (idx) => {
        setField(field.key, rules.filter((_, i) => i !== idx));
      };
      const addRule = () => {
        setField(field.key, [...rules, ["", ""]]);
      };
      return (
        <div className="space-y-2">
          {rules.length === 0 && (
            <div className="text-xs text-slate-500 bg-slate-50/60 border border-slate-200 rounded-lg px-3 py-2">
              暂无规则，点击下方按钮新增
            </div>
          )}
          {rules.map((rule, idx) => (
            <div key={idx} className="flex items-center gap-2">
              <input
                type="text"
                value={rule[0]}
                onChange={(e) => updateRule(idx, 0, e.target.value)}
                placeholder="匹配正则，如 ^gpt-4$"
                className="flex-1 min-w-0 px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-900 placeholder-slate-400 font-mono"
              />
              <span className="text-slate-500 text-sm shrink-0">→</span>
              <input
                type="text"
                value={rule[1]}
                onChange={(e) => updateRule(idx, 1, e.target.value)}
                placeholder="替换模型名，如 gemini-2.5-pro"
                className="flex-1 min-w-0 px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-900 placeholder-slate-400 font-mono"
              />
              <button
                type="button"
                onClick={() => removeRule(idx)}
                className="shrink-0 p-2 text-slate-500 hover:text-red-700 transition-colors"
                title="删除该规则"
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
          <button
            type="button"
            onClick={addRule}
            className="btn btn-secondary text-xs px-3 py-1.5 flex items-center gap-1.5"
          >
            <Plus size={14} />
            新增规则
          </button>
        </div>
      );
    }

    if (field.type === "textarea") {
      return (
        <LinesField
          value={value}
          onChange={(v) => setField(field.key, v)}
          placeholder={field.placeholder}
          rows={6}
          className="w-full px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-900 placeholder-slate-400 font-mono"
        />
      );
    }

    return (
      <input
        type={field.type}
        aria-label={field.label}
        min={field.min}
        max={field.max}
        step={field.step}
        value={value}
        onChange={(e) =>
          setField(
            field.key,
            field.type === "number" ? Number(e.target.value) : e.target.value
          )
        }
        placeholder={field.placeholder}
        className="w-full px-3 py-2 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-900 placeholder-slate-400"
      />
    );
  };

  return (
    <div className="space-y-6">
      {/* 操作按钮（sticky 常驻视口顶部） */}
      <div className="sticky top-0 z-30 -mx-1 px-1 py-2 bg-slate-50/95 backdrop-blur border-b border-slate-200 rounded-lg">
        <div className="flex items-center gap-2">
        <button
          onClick={loadConfig}
          disabled={loading}
          className="btn btn-secondary text-sm flex items-center gap-1.5"
        >
          <RefreshCw size={15} className={loading ? "animate-spin" : ""} />
          刷新配置
        </button>
        <button
          onClick={saveConfig}
          disabled={saving || loading}
          className="btn btn-primary text-sm flex items-center gap-1.5 disabled:opacity-50"
        >
          {saving ? <Loader2 size={15} className="animate-spin" /> : <Save size={15} />}
          {saving ? "保存中..." : "保存配置"}
        </button>
        </div>
      </div>

      {/* 消息弹窗 */}
      {message && (
        <Modal type={message.type} title={message.type === "success" ? "操作成功" : "操作失败"} onClose={() => setMessage(null)}>
          <span className="whitespace-pre-wrap break-all">{message.text}</span>
        </Modal>
      )}

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="animate-spin text-purple-500" size={28} />
        </div>
      ) : (
        <div className="space-y-6">
          {CONFIG_GROUPS.map((group) => (
            <div key={group.title} className="card">
              <h3 className="text-base font-semibold text-purple-700 mb-4 border-b border-slate-200 pb-3">
                {group.title}
              </h3>

              {group.title === "499 等待异常保护" && (
                <div className="mb-4 text-sm space-y-2">
                  <button type="button" className="btn btn-secondary text-xs" onClick={loadCooldownStatus}>刷新保护状态</button>
                  <p className="text-slate-600">{cooldownStatus ? `已保存模式：${{off: "关闭", observe: "仅观察", enforce: "执行冷却"}[cooldownStatus.mode]}；下方为最近一次刷新的状态。` : "保护状态暂时无法读取"}</p>
                  {cooldownStatus?.items.length === 0 && <p className="text-slate-500">暂无符合触发条件的异常记录。</p>}
                  {!!cooldownStatus?.items.length && <div className="overflow-x-auto"><table className="w-full text-left text-xs">
                    <thead><tr>{["账号 / 范围", "状态", "窗口内异常", "在途请求", "冷却结束", "触发 / 停发机会"].map(label => <th className="p-2" key={label}>{label}</th>)}</tr></thead>
                    <tbody>{cooldownStatus.items.map(item => <tr key={`${item.email}/${item.group}`} className="border-t border-slate-200">
                      <td className="p-2">{item.email} / {item.group_label || (item.group === "*" ? "整个账号" : item.group)}</td>
                      <td className="p-2">{{closed: "正常", draining: "等待在途结束", cooling: "冷却中", probe: "等待恢复试探"}[item.phase]}</td>
                      <td className="p-2">{item.failure_count}</td><td className="p-2">{item.active}</td>
                      <td className="p-2">{item.until ? new Date(item.until * 1000).toLocaleString() : "—"}</td>
                      <td className="p-2">{item.trips} / {item.would_block}</td>
                    </tr>)}</tbody>
                  </table></div>}
                  <p className="text-xs text-slate-500">仅观察模式显示模拟保护状态，实际请求继续派发。其他服务或不同数据库上的调用不受此处控制。</p>
                </div>
              )}

              {group.title === "端点配置" && (
                <div className="flex flex-wrap gap-2 mb-4">
                  <button
                    type="button"
                    onClick={useMirrorUrls}
                    className="btn btn-secondary text-xs px-3 py-1.5"
                  >
                    🚀 一键使用镜像网址
                  </button>
                  <button
                    type="button"
                    onClick={restoreOfficialUrls}
                    className="btn btn-secondary text-xs px-3 py-1.5"
                  >
                    🔄 还原官方端点
                  </button>
                </div>
              )}

              <div className="space-y-5">
                {group.fields
                  .filter((field) => !field.showIf || field.showIf(config))
                  .map((field) => (
                    <div key={field.key}>
                      <label className="block text-sm font-medium text-slate-700 mb-1.5">
                        {field.label}
                      </label>
                      {renderField(field)}
                      {field.note && (
                        <p className="text-xs text-slate-500 mt-1.5">{field.note}</p>
                      )}
                    </div>
                  ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
