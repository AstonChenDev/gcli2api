# AGY2API

> 本分支保存定制版代码，已同步至 2026-10-28 的 `bcb264b-clipboard-20261028` 发布版本。
> 包含浅色界面、凭证批量测试和选中导出、额度展示、RT 导入、逐凭证模型统计、
> 思考流兼容处理、Ultra Lite 标注，以及 HTTP 访问时复制请求明细 JSON 的修复。
> 下文的上游镜像和安装脚本不会安装这些定制功能；请使用本仓库源码及根目录 Dockerfile 构建。
> 生产配置、密码、凭证和数据库不包含在仓库内，环境变量格式见 `.env.example`。

**将 Google Antigravity 转换为 OpenAI、Gemini 和 Claude API 兼容接口**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: CNC-1.0](https://img.shields.io/badge/License-CNC--1.0-red.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-available-blue.svg)](https://github.com/su-kaka/agy2api/pkgs/container/agy2api)

[English](docs/README_EN.md) | 中文 | [日本語](docs/README_JA.md)

> **⚠️ 重要变更**：GeminiCLI 端点已被谷歌抛弃，本项目已完全移除 GeminiCLI 相关代码，仅保留 Antigravity 模式。

## 凭证权益自动刷新（定制功能）

服务启动后在后台分批查询每个凭证（包括已禁用凭证）的 `loadCodeAssist` 和 `fetchAvailableModels`，成功后约每 5 分钟刷新。等级以 `paidTier.id` 优先、缺少付费权益时使用 `currentTier.id`，避免将仍享有 Ultra 权益的账号误判为 Free。上游明确返回 `free-tier` 或 `free` 时，在同一数据库事务内将会员等级更新为 Free 并禁用该凭证；即使本次模型目录查询失败，也会执行已经确认的 Free 禁用。其他明确等级正常更新，备注及原有冷却保持；账号后来升级也不会自动重新启用，需手动确认后启用。缺失、未知或不完整的等级数据，以及查询超时、认证失败、429/404 等错误，均不能作为 Free 自动禁用的依据。

权限快照及刷新租约保存在独立的 `credential_entitlements` 表，多个 worker 共用。每个 worker 每轮最多刷新 2 个凭证，每 5 秒检查待刷新任务，不为每次生成请求重复查询上游。404 会提示后台提前刷新，同一账号的两次刷新至少间隔 60 秒；404 本身不会被当作账号封禁依据，也不会自动重放已开始的生成。

凭证卡片的“查看可用模型”展示该邮箱的模型权限目录、备注、会员等级、最近成功查询时间与查询异常，支持搜索及复制完整模型名。打开弹窗只读取已有快照；“刷新权限”复用上述后台刷新规则和跨 worker 限流，更新会员信息时仍遵循明确 Free 自动禁用、升级不自动启用的规则。禁用凭证也能查看，查询失败保留历史目录；“尚未查询成功”与“已确认没有可用模型”分别显示。目录只列出上游真实型号，不把未配置的基础名或别名当作该凭证的权限；有模型权限不代表额度足够或请求必定成功，配额冷却单独标注，剩余额度使用原“额度”入口查询。面板鉴权接口为 `GET /creds/models/{email}` 和 `POST /creds/models/{email}/refresh`。

随机、会话缓存、粘性选择及最终派发均会跳过已确认不支持目标型号的凭证。筛选使用完成别名和思考等级转换后的实际上游型号。未知权限允许沿用原调度，随机选择优先使用已确认支持的账号；启动预热期间或权限变化尚未刷新时仍可能遇到一次上游拒绝。查询超时、401、429、异常结构等不会清空最后一次有效权限或误降级为 Free，后台稍后重试。此机制不替代额度冷却，也不保证消除所有 404/429。

## 安装指南

### Termux 环境

**初始安装**
```bash
curl -o termux-install.sh "https://raw.githubusercontent.com/su-kaka/agy2api/refs/heads/master/termux-install.sh" && chmod +x termux-install.sh && ./termux-install.sh
```

**重启服务**
```bash
cd agy2api
bash termux-start.sh
```

### Windows 环境

**初始安装**
```powershell
iex (iwr "https://raw.githubusercontent.com/su-kaka/agy2api/refs/heads/master/install.ps1" -UseBasicParsing).Content
```

**重启服务**
双击执行 `start.bat`

### Linux 环境

**初始安装**
```bash
curl -o install.sh "https://raw.githubusercontent.com/su-kaka/agy2api/refs/heads/master/install.sh" && chmod +x install.sh && ./install.sh
```

**重启服务**
```bash
cd agy2api
bash start.sh
```

### macOS 环境

**初始安装**
```bash
curl -o darwin-install.sh "https://raw.githubusercontent.com/su-kaka/agy2api/refs/heads/master/darwin-install.sh" && chmod +x darwin-install.sh && ./darwin-install.sh
```

**重启服务**
```bash
cd agy2api
bash start.sh
```

### Docker 环境

**Docker 运行命令**
```bash
docker run -d --name agy2api --network host -e API_PASSWORD=api_pwd -e PANEL_PASSWORD=panel_pwd -e PORT=7861 -v $(pwd)/data/creds:/app/creds -v $(pwd)/data/request_logs:/app/data/request_logs ghcr.io/su-kaka/agy2api:latest
```

**Docker Compose 运行命令**
1. 使用仓库中的 `docker-compose.yml`，或参考以下内容：
    ```yaml
    version: '3.8'

    services:
      agy2api:
        image: ghcr.io/su-kaka/agy2api:latest
        container_name: agy2api
        restart: unless-stopped
        network_mode: host
        environment:
          # 密码配置（分离密码）
          - API_PASSWORD=your_api_password
          - PANEL_PASSWORD=your_panel_password
          - PORT=7861
        volumes:
          - ./data/creds:/app/creds
          - ./data/request_logs:/app/data/request_logs
    ```
2. 启动服务：
    ```bash
    docker-compose up -d
    ```

## 核心功能

### 🔄 API 端点和格式支持

**Antigravity 多端点多格式支持**
- **OpenAI 格式端点**：`/v1/chat/completions` 和 `/v1/models`
- **Gemini 原生端点**：`/v1/models/{model}:generateContent` 和 `streamGenerateContent`
- **Claude 格式端点**：`/v1/messages`
- 支持所有 Antigravity 模型（Claude、Gemini 等）
- 自动模型名称映射和思维模式检测
- 多种认证方式：Bearer 头部、x-api-key、x-goog-api-key、URL 参数 key

### 📊 智能凭证管理系统

- 多个 Google OAuth 凭证自动轮换（随机负载均衡）
- 模型级冷却（429 错误自动设置对应模型家族的 CD：argon / gemini / claude+gpt 共享冷却）
- 自动故障检测和凭证禁用
- 凭证使用统计和配额/额度查询
- 批量操作（启用、禁用、删除）
- 以邮箱为唯一键，重复导入自动覆盖去重
- 自动获取 Google 账户邮箱地址、订阅等级（Tier）和 Project ID

### 🌊 流式传输和响应处理

- 真正的实时流式响应（SSE）
- 流式抗截断功能（防止回答被截断，通过"抗截断-"前缀启用）
- 思维链（Thinking）内容分离

### 🎛️ Web 管理控制台

基于 React + Vite + Tailwind 编译的现代化前端界面：
- Antigravity OAuth 认证流程（支持回调 URL 快捷方式）
- 凭证文件上传（JSON/ZIP）、下载、详情查看
- 凭证导出（规范 JSON：refresh_token/access_token/expiry/email/tier/project_id）
- 系统配置管理（环境变量锁定提示、热更新）
- 批量凭证操作和状态筛选、批量导入（缺失字段自动获取）
- 移动端自适应

## 支持的模型

模型列表从 Antigravity API 动态获取，支持所有 Antigravity 模型：

- **Claude 系列**：`claude-sonnet-4-5`、`claude-opus-4-5` 等
- **Gemini 系列**：`gemini-2.5-flash`、`gemini-3-flash-preview` 等

**特殊功能变体：**
- **抗截断模式**：在模型名称前添加 `抗截断-` 前缀
  - 例：`抗截断-gemini-2.5-pro`
  - 通过合成工具调用绕过截断限制，确保完整回答（流式和非流式均有效）
- **图像生成模型**支持分辨率（`-2k`、`-4k`）和比例（`-16x9`、`-9x16` 等）后缀

## 配置说明

1. 访问 `http://127.0.0.1:7861`（默认端口，可通过 PORT 环境变量修改）
2. 完成 Antigravity OAuth 认证流程（默认密码：`pwd`，可通过环境变量修改）
3. 配置客户端：

**OpenAI 兼容客户端：**
- **端点地址**：`http://127.0.0.1:7861/v1`
- **API 密钥**：`pwd`（默认值，可通过 API_PASSWORD 环境变量修改）

**Gemini 原生客户端：**
- **端点地址**：`http://127.0.0.1:7861`
- **认证方式**：`Authorization: Bearer 密钥`、`x-goog-api-key: 密钥` 或 URL 参数 `?key=密钥`

**Claude 客户端：**
- **端点地址**：`http://127.0.0.1:7861/v1/messages`
- **认证方式**：`x-api-key: 密钥` 或 `Authorization: Bearer 密钥`

## 💾 数据存储

### 存储后端

| 后端 | 说明 | 启用方式 |
|------|------|----------|
| **SQLite**（默认） | 本地文件存储，开箱即用 | 无需配置，数据存储在 `creds/credentials.db` |
| **PostgreSQL** | 云端分布式存储，多实例部署 | 设置 `POSTGRESQL_URI` 环境变量 |

### 凭证数据结构

凭证以**邮箱（email）为唯一键**存储，字段为结构化数据库列（不再使用 JSON blob）：
`email`、`access_token`、`refresh_token`、`client_id`、`client_secret`、`project_id`、`expiry`、`tier`

凭证导入/导出使用规范 JSON 格式：`{"refresh_token": "...", "access_token": "...", "expiry": "...", "email": "...", "tier": "...", "project_id": "..."}`（仅 `refresh_token` 必填，`access_token` 未过期时直接复用，其余字段可自动获取）。

### 环境变量配置

**基础配置**
- `PORT`: 服务端口（默认：7861）
- `HOST`: 服务器监听地址（默认：0.0.0.0）

**密码配置**
- `API_PASSWORD`: 聊天 API 访问密码（默认：pwd）
- `PANEL_PASSWORD`: 控制面板访问密码（默认：pwd）

**访问控制配置**
- `IP_WHITELIST_ENABLED`: 启用 IP 白名单（默认：false，不限制）
- `IP_WHITELIST`: IP 白名单列表，逗号或换行分隔（默认：空）。每条支持单个 IP（`192.168.1.1`）、CIDR 网段（`192.168.1.0/24`）、起止范围（`10.0.0.1-10.0.0.100`），IPv4 / IPv6 均可。开启后仅白名单内 IP 可访问面板和 API，其余 IP 直接 403；面板保存后立即生效

**模型名复写配置**
- `MODEL_REWRITE_ENABLED`: 启用模型名复写（默认：false）
- `MODEL_REWRITE_RULES`: 复写规则，每条为 `[匹配正则, 替换模型名]`（默认：空）。自上而下首条命中即替换，替换支持正则反向引用（如 `\1`）。例：把 `gpt-4o` 映射到 `gemini-2.5-pro`。面板保存后立即生效

**存储配置**
- `CREDENTIALS_DIR`: 凭证数据库目录（默认：./creds）
- `POSTGRESQL_URI`: PostgreSQL 连接字符串（设置后启用 PostgreSQL 模式）

**网络和代理配置**
- `PROXY`: 代理地址（支持 `http://`、`https://`、`socks5://`、`socks5h://`，可带账号密码 `user:pass@host:port`）
- `OAUTH_PROXY_URL`: OAuth 认证代理端点
- `GOOGLEAPIS_PROXY_URL`: Google APIs 代理端点
- `ANTIGRAVITY_API_URL`: Antigravity API 端点
- `PATH_PREFIX`: 反代保留路径前缀（如 `/AGY2API`）时配置，由后端在路由匹配前剥掉该前缀；默认：空（不剥离）

**性能和稳定性配置**
- `RETRY_429_ENABLED`: 启用错误自动重试（默认：true）
- `RETRY_429_MAX_RETRIES`: 最大重试次数（默认：5）
- `RETRY_429_INTERVAL`: 重试间隔，秒（默认：1）
- `ANTI_TRUNCATION_MAX_ATTEMPTS`: 抗截断最大重试次数（默认：3）

**自动化配置**
- `AUTO_BAN`: 启用凭证自动封禁（默认：false）
- `AUTO_BAN_ERROR_CODES`: 触发自动封禁的错误码（默认：403）

**响应行为配置**
- `RETURN_THOUGHTS_TO_FRONTEND`: 返回思维链到前端（默认：true）

**敏感词过滤配置**
- `SENSITIVE_WORDS_ENABLED`: 启用敏感词过滤（默认：false）
- `SENSITIVE_WORDS`: 敏感词列表，逗号或换行分隔（默认：空）。命中后整词替换为空，不区分大小写，支持中文，面板保存后立即生效

**请求日志配置**
- `USAGE_LOG_ENABLED`: 启用请求日志（默认：true）。记录每次 API 请求（模型、凭证、token 用量、耗时、状态）到数据库，不自动清理，可在面板手动清理或清空

**请求体详细日志配置**
- `REQUEST_LOG_ENABLED`: 启用请求体详细日志（默认：false）。开启后，当请求命中指定错误码时，把发往上游的完整请求头（已脱敏）与请求体保存为 JSON 文件到 `data/request_logs/`，数据库仅记录文件索引，在"请求日志"详情中查看。主要用于请求体错误时排查 debug；清理/清空日志时会同步删除对应文件（Docker 部署请挂载 `./data/request_logs:/app/data/request_logs` 以持久化）
- `REQUEST_LOG_ERROR_CODES`: 触发详细日志的错误码列表，逗号分隔（默认：400）

**凭证选择模式**
- `CREDENTIAL_SELECT_MODE`: 凭证选择模式（默认：cache）
  - `random` — 完全随机负载均衡，每次请求随机挑选凭证
  - `cache` — 缓存匹配（推荐）：以"模型 + system 指令（或首条 user 消息）"哈希会话 key，相同会话复用同一凭证，命中 Google 上游 prompt cache；凭证 CD 后自动切换并重新绑定
  - `sticky` — 凭证粘性：所有请求锁定同一凭证，直到其进入 CD 或被禁用
- `REDIS_URL`: Redis 连接串（可选）。设置后缓存匹配的会话绑定存入 Redis，多实例共享；未设置时使用进程内存后端

**日志配置**
- `LOG_LEVEL`: 日志级别（DEBUG/INFO/WARNING/ERROR，默认：INFO）
- `ENABLE_LOG`: 完全静默，不打印任何日志（设为 0 启用静默，默认：1）

## API 使用示例

### OpenAI 格式

```bash
curl -X POST "http://127.0.0.1:7861/v1/chat/completions" \
  -H "Authorization: Bearer your_api_password" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-5",
    "messages": [
      {"role": "user", "content": "Hello"}
    ],
    "stream": true
  }'
```

### Gemini 原生格式

```bash
curl -X POST "http://127.0.0.1:7861/v1/models/gemini-2.5-flash:generateContent?key=your_api_password" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [
      {"role": "user", "parts": [{"text": "Hello"}]}
    ]
  }'
```

### Claude 格式

```bash
curl -X POST "http://127.0.0.1:7861/v1/messages" \
  -H "x-api-key: your_api_password" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 1024,
    "messages": [
      {"role": "user", "content": "Hello, Claude!"}
    ]
  }'
```

### 图像生成（Gemini 原生 SDK）

```python
from io import BytesIO
from PIL import Image
from google.genai import Client
from google.genai.types import HttpOptions
from google.genai import types

client = Client(
    api_key="pwd",
    http_options=HttpOptions(base_url="http://127.0.0.1:7861"),
)

prompt = "画一只猫"

response = client.models.generate_content(
    model="gemini-3.1-flash-image",
    contents=[prompt],
    config=types.GenerateContentConfig(
        image_config=types.ImageConfig(
            aspect_ratio="16:9",
        )
    )
)
for part in response.candidates[0].content.parts:
    if part.text is not None:
        print(part.text)
    elif part.inline_data is not None:
        image = Image.open(BytesIO(part.inline_data.data))
        image.save("generated_image.png")
```

## 🛠️ 前端开发

控制面板前端源码位于 `frontend/` 目录（React + Vite + Tailwind CSS）：

```bash
cd frontend
pnpm install
pnpm run dev     # 开发模式，端口 3000，代理后端 API
pnpm run build    # 构建到 dist/（生产模式使用）
```

后端直接托管 `dist/` 下编译后的静态文件，无需 Node.js 运行时。

## 💬 交流群

欢迎加入 QQ 群交流讨论！

**QQ 群号：1083250744**

<img src="docs/qq-group.jpg" width="200" alt="QQ 群二维码">

## 许可证与免责声明

本项目仅供学习研究使用，禁止商业用途和倒卖。

## ⚠️ 许可证声明

本项目使用 [CNC-1.0 许可证](LICENSE)（反商业许可证）。

### 本地凭证备注、ID 查询与 Token 统计

- 凭证管理支持新增、编辑、清空备注（最多 1000 字）；刷新凭证不会覆盖备注。
- 请求日志支持精确查询 Request ID、日志数字 ID 和上游 `responseId`，同一请求的每次上游重试分别显示。选择结果后点击“查看详细内容”可查看并复制 JSON。
- 配置管理 → 请求日志：开启“启用请求日志”“启用请求体详细日志”和“保存全部新请求体”，可保存成功、失败模型调用的请求体。环境变量对应 `USAGE_LOG_ENABLED`、`REQUEST_LOG_ENABLED`、`REQUEST_LOG_ALL_REQUESTS`。全量记录开关默认关闭，未开启时仍遵循原有错误码筛选。
- 保存的是发往上游的请求体（含协议转换）；尚未派发上游的凭证获取失败保存当前请求参数。敏感认证头已脱敏。JSON 在调用结束时保存；历史未保存、已清理的内容无法补回，上游未返回的 responseId 不会生成替代值。
- 数据统计按凭证和模型显示输入 / 输出 M；1 M = 1,000,000 Tokens。输入包含缓存命中，输出包含思考 Tokens。用量来自上游返回的统计，未返回的用量无法计入；清理日志会同时减少统计。
- 启动时为当前 email 键凭证表增加备注列、为请求日志增加 responseId 列和查询索引，保留现有记录。

### ZIP / JSON 导入进度

批量导入现在逐项显示结果，使用 3 个并发验证任务；刷新令牌每次最多等待 20 秒，网络或暂时服务错误自动重试一次，单个凭证总验证预算 90 秒。前端通过 `/creds/upload-stream` 接收进度和心跳，不再受原来 120 秒的整批等待限制。停止导入或离开页面会取消未完成任务，已经保存的凭证保留；失败项会显示具体原因，网络失败不会标为令牌无效。原 `/creds/upload` JSON 接口保留，使用相同的限流和超时逻辑。单次上传上限 20 MB、100 个外部文件、1000 个凭证，JSON 总大小不超过 64 MB；ZIP 解析在后台线程执行，编码或格式错误逐项显示。

流式导入在收到断连通知或发送失败时，会关闭进度生成器并等待未完成任务退出；ASGI 2.4 在验证期间也监听断连，无需等待下一次心跳发送。

### 系统提示词字段兼容

Gemini 请求的 `system_instruction` 与 `systemInstruction` 会先统一为一个 `systemInstruction` 字段，保留原始 parts；不追加私有思考分隔提示。抗截断模式仍按原有流程处理。两种写法同时传入且内容相同会去重；内容不同则在请求入口返回明确的 400，避免悄悄丢弃任意一方提示词。普通流式、非流式和抗截断路径使用相同字段兼容规则。

### 二开回归补充

- Gemini 已声明的下划线参数（generation_config、tool_config，以及 thinking_config / include_thoughts 等）统一为对应协议字段，避免重复字段及思考开关被忽略；用户 JSON Schema 属性名、工具参数内容不改写。
- 流式调用每次上游尝试仅记账一次；已记失败的流被关闭时不再追加成功记录。客户端取消记为 499 失败并保留已收到的用量，日志收尾在取消作用域内受保护。
- 选中导出使用真实保存的凭证等级；单个、全部与选中导出均携带非空备注。导入含 note 的备份会原样恢复备注，包括首尾空格和换行；旧备份没有 note 时保留现有备注。

### 流式完成与 499 记账

流式响应已输出有效的 `finishReason`（多候选需全部结束）、`[DONE]` 或上游正常读完后，随后关闭连接不会再误记为 499。仅识别到正文分界不代表响应完成；在结束前取消仍记为 499。日志收尾保留已确定的成功或错误结果，避免取消期间变更状态、重复记账。诊断元数据记录结束原因、结束标记及 EOF，不记录响应正文。此规则按服务端可观察的协议状态判断，不追溯改写缺少结束标记证据的历史 499。


### 按思考等级匹配模型

在后台「配置 → 思考等级模型匹配」开启开关并保存，立即对新请求生效。默认关闭；未配置过适用模型时，默认包含 `gemini-4-argon`、`gemini-4-carbon`、`gemini-4-hydrogen`。已有的后台名单和环境变量保持不变，升级后需要使用 Hydrogen 时，须在实际生效的名单中追加 `gemini-4-hydrogen`。可按行添加其他基础模型名，但应先确认上游提供对应的等级变体。

OpenAI `/v1/chat/completions` 请求示例：

```json
{
  "model": "gemini-4-argon",
  "reasoning_effort": "high",
  "messages": [{"role": "user", "content": "请解释这段代码"}],
  "stream": true
}
```

此请求选择 `gemini-4-argon-high`。将等级改为 `medium` 或 `low` 即选择对应变体。流式、非流式及 `抗截断-` 前缀均支持。

Hydrogen 额外支持 `xhigh`：`"model": "gemini-4-hydrogen"` 配合 `"reasoning_effort": "xhigh"` 会选择 `gemini-4-hydrogen-xhigh`；配合 `medium` 则选择 `gemini-4-hydrogen-medium`。Argon、Carbon 和其他模型仍只接受原有三档。

- OpenAI：`reasoning_effort`，也接受 `reasoning.effort`（仅聊天接口，不新增 Responses 接口）。
- Gemini：路径模型名使用基础名称，请求体传 `generationConfig.thinkingConfig.thinkingLevel`，也接受对应 snake_case 字段。
- Claude 格式 `/v1/messages`：基础模型名配合 `output_config.effort`。
- Hydrogen 接受 low / medium / high / xhigh，其他模型接受 high / medium / low，不区分大小写；仅对已配置的基础名称生效。未传等级（或等级为 null）时统一默认选择 medium，例如 gemini-4-hydrogen 自动匹配 gemini-4-hydrogen-medium。非法或相互冲突的等级返回 HTTP 400。
- 已经带后缀的模型名优先，例如 `gemini-4-argon-low` 配 `high` 仍使用 low。其他模型保持原行为。
- 匹配发生在模型名复写规则之前；已有复写规则仍可覆盖匹配结果。请求日志保留客户端原始模型名，上游请求使用匹配后的模型名。
- 开关开启且模型列表包含该基础模型全部受支持的等级变体时（Hydrogen 四档，其他模型三档），模型列表额外显示基础名称，保留原有变体。

环境变量（优先于后台设置，按现有配置机制锁定对应后台项）：

```dotenv
THINKING_MODEL_ROUTING_ENABLED=true
THINKING_MODEL_ROUTING_BASES=gemini-4-argon,gemini-4-carbon,gemini-4-hydrogen
```

多个基础模型用英文逗号分隔；空列表表示不匹配任何模型。此功能负责选择上游模型，不保证所有凭证都具有该模型权限。

### 原项目的思考处理规则

- 按上游原生 `thought` 字段转换思考与正文：OpenAI 格式使用 `reasoning_content`，Anthropic 格式使用 thinking 内容块，Gemini 格式保留原生字段。未标注为思考的文字按正文传递，不根据标题、语言、标签或内容自动重分类、隐藏或删除。
- “返回思维链到前端”开关按原项目规则作用于请求中的 `includeThoughts`（适用的模型配置）。如果上游仍返回思考字段，按原生字段转发，不额外做本地强制过滤。
- 已移除二开的私有分隔提示注入、`AGY_FINAL` 标记处理、正文边界恢复及被动分析识别。`ARGON_THOUGHT_BOUNDARY_ENABLED` 和 `ARGON_PASSIVE_THOUGHT_RECOVERY_ENABLED` 已失效，即使环境仍设为 `true` 也不会重新开启。用户原有系统提示及返回文字保持原样。
- 移除提示注入消除了本次已复现的输入触发项；配额、容量等其他原因仍可能产生 429。原生签名、工具调用、流式错误处理、完成状态与用量统计保留。

### 流式连接与日志清理

- 独立签名在有效输出之前暂存并按顺序转发；不提前提交成功响应，只有签名而无有效内容仍按空回复处理，重试之间不混用签名。失败的上游流在重试前释放，取消或结束时也会回收。
- 抗截断达到续写次数上限时，合成工具正文仍因 Token 上限截断的会保留 `MAX_TOKENS`，OpenAI / Claude 格式分别返回 `length` / `max_tokens`；成功补全后才返回正常结束。

- 在“配置管理 → 基础配置”中，可通过“启用握手超时”和“握手超时秒数”控制流式上游调用的建连阶段（TCP、代理协商、TLS）。默认开启、40 秒，可设置 1–3600 秒，关闭后取消建连时限但仍保留断连回收。保存后对新请求生效，已建立的请求保持原设置。
- 开始发送模型请求后解除握手期限，正常生成与流式读取默认不限时。每次重试单独计时，总请求耗时可能超过设置秒数。也可通过 `STREAM_HANDSHAKE_TIMEOUT_ENABLED` 和 `STREAM_HANDSHAKE_TIMEOUT_SECONDS` 环境变量设置，环境变量会锁定对应后台选项。

### 499 等待异常保护

凭证管理支持单个“清除 CD”和勾选后的“批量清除 CD”。可选择全部冷却、仅配额冷却（所有模型家族）、仅 499 保护（账号及所有家族）。清除 499 保护会重置本轮失败计数、冷却和试探状态，保留累计触发统计与正在进行的请求，并隔离清除前请求的保护结果。禁用状态、错误记录和凭证内容不变。此操作只解除本服务的冷却，不恢复上游配额；新的限制仍会重新触发冷却。

删除凭证同时重置其 499 保护状态；重新导入同一邮箱不会继承旧冷却。删除前已经派发的请求继续运行，但它们的 499 保护结果不会影响重新导入的凭证。按配额 CD 筛选时，清除操作导致当前页消失会自动回到有效页码。

在“配置管理 → 499 等待异常保护”编辑并保存以下参数。默认**仅观察**，不会停止派发；选择“执行冷却”后才实施限制。499 本身不等于上游限流，20 分钟闲置也不是已证实的恢复承诺。

| 配置项 | 默认值 | 可选范围 |
| --- | --- | --- |
| `cooldown_499_mode` | `observe` | `off` 关闭 / `observe` 仅观察 / `enforce` 执行冷却 |
| `cooldown_499_threshold` | 3 次 | 1–100 |
| `cooldown_499_window_seconds` | 300 秒 | 1–86400 |
| `cooldown_499_min_wait_seconds` | 90 秒 | 0–3600 |
| `cooldown_499_duration_seconds` | 1200 秒 | 1–86400 |
| `cooldown_499_scope` | `account` | `account` 整个账号 / `family` 当前模型家族 |
| `cooldown_499_probe_concurrency` | 1 | 1–32 |
| `cooldown_499_custom_families_enabled` | `false` | 是否启用自定义模型家族 |
| `cooldown_499_family_rules` | `[]` | 有序分组列表，每组包含 `name` 和 `patterns` |
| `cooldown_499_family_fallback` | `default` | `default` 沿用默认家族 / `model` 未匹配模型分别冷却 |

冷却范围选择“当前模型家族”后，可开启“启用自定义模型家族”，添加分组、修改名称与匹配列表，使用上移/下移调整优先级。每组模型规则一行一条，按完整模型名匹配，不区分大小写；`*` 匹配任意字符，`?` 匹配一个字符，不支持正则或方括号字符集合。最多 32 组，每组 1–32 条规则；组名最多 64 字且不能重复，单条规则最多 128 字。

例如第一组“Argon 高档”填写 `gemini-4-argon-high*`，第二组“其他 Argon”填写 `*argon*`，即可让 high 与 medium/low 分开冷却；在同一组填写多个不同家族的模型，也可让它们共用 499 冷却。多组命中只取第一组。自定义组与默认组相互独立，同名也不会自动合并。

“预览匹配”使用当前编辑内容，可以在保存前确认分组；只检查规则，不修改数据库、不发送生成请求。匹配依据是模型路由、模型名复写后实际发往上游的请求模型名，不是响应中报告的 `modelVersion`。自定义规则只改变 499 保护分组，429/503 配额冷却仍使用原分组。冷却范围为“整个账号”时，自定义分组不缩小账号保护范围。

- 只有实际下游断开、最终状态为 499、等待达到最低时间、上游没有任何事件（包括思考、心跳和元数据）、下游没有输出的请求计入。快速取消、已有输出、转换错误不会计入。完整成功清空窗口内累计；同一请求、同一账号在窗口内去重。统计不依赖请求日志开关。
- 触发后不再派发新生成请求，已有请求继续完成，最后一个在途请求结束后才开始冷却。到期只允许指定并发量的自然业务请求试探；完整成功恢复派发，符合条件的失败重新冷却，其他错误仍遵循原有错误处理。不会额外定时生成或重试已断开的请求。
- 状态单独持久化在 `credential_499_state`；不会修改凭证的 `disabled` 或混用 429/503 的模型冷却。SQLite 写事务 / PostgreSQL 行锁保证共享同一数据库的进程原子派发。缓存、粘性、预取及面板手动测试都不能绕过保护。全部候选不可派发时返回 503 和 `Retry-After`；等待在途结束或试探占满时，1 秒表示建议重新检查，不能保证届时有空位。
- 所有参数热更新。模式、范围、自定义分组开关、规则或未匹配处理变更会清空保护历史，旧请求的结果不能触发新策略，但仍按新分组参与在途计数；旧版本未记录请求模型名的在途请求保守地计入所有家族，直到结束或所有权过期。其他参数不清空历史。最低等待时间使用该请求派发时的值；已经触发的冷却保留原时长，修改并发上限不取消已派发的试探。
- 在途所有权每 30 秒续期，120 秒无续期视为进程失联；失联试探不会当作恢复成功。过期在下一次派发、结算或状态刷新时处理，保守地重新开始完整冷却。面板状态为刷新时的快照；观察模式的停发机会不是“避免失败数”。
- 保护覆盖本服务的生成请求，不覆盖外部客户端、不同数据库的实例或旧版进程，也不阻止 OAuth / 配额查询。因此整个账号真正闲置仍要求其余调用方共同遵守。
- 客户端在首个响应前或流式输出期间断开时，取消对应任务并关闭上游连接。建连超时记录为 504 / `UPSTREAM_CONNECT_TIMEOUT`，已观察到的客户端断开记录为 499；完整交付后的断开仍按成功处理。
- 清空日志使用事务删除已有记录，保留表、索引及操作期间新增的记录。详细文件只删除已删除记录对应的文件；尚未入库的文件和孤立文件不会被整目录清除。
- OpenAI / Claude 响应中异常的 token 统计字段不会丢弃正文；无法恢复的流式转换错误返回 `RESPONSE_CONVERSION_FAILED` 并记录 502，已开始输出时通过流内错误结束，不误记为客户端断开 499。客户端在获取凭证阶段断开也记录日志，重试预取任务随请求退出回收。
- 非流式上游错误保留原状态码、正文及 `Retry-After`，空响应或 HTML 错误不会在 OpenAI / Claude 转换时被改成 500。解压后的响应重新计算长度，移除旧压缩头及逐跳连接头，避免客户端二次解压失败。
- OpenAI 多候选响应分别维护工具调用序号和结束状态，某个候选结束不清空其他候选的状态；省略候选编号时使用其数组位置。普通流式与抗截断请求使用各自的工具计数，避免共享缓存淘汰在途状态；`FINISH_REASON_UNSPECIFIED` 不作为结束标记。
- 凭证列表默认每页显示 50 个，仍可在页面切换每页数量。
