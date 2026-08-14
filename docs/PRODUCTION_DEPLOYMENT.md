# GCLI2API 生产部署

本项目使用同一份 `Dockerfile` 和 `docker-compose.yml` 部署到两台生产服务器。服务器之间的差异只保存在各自未入库的 `.env` 文件中。

## 部署结构

| 配置 | production-1 (`43.160.230.122`) | production-2 (`43.162.94.148`) |
| --- | --- | --- |
| `COMPOSE_PROFILES` | `candidate,v3` | `candidate` |
| 运行端口 | `7861`、`7862`、`7863` | `7861`、`7862` |
| PostgreSQL | 本机 `15432` | 本机 `15432` |
| COS | 与 production-2 相同 | 与 production-1 相同 |

两台服务器都可以把 PostgreSQL 地址统一写成：

```dotenv
POSTGRESQL_URI=postgresql://USER:URL_ENCODED_PASSWORD@host.docker.internal:15432/gcli2api
```

Compose 已配置 `host.docker.internal:host-gateway`，无需再使用服务器公网 IP 回连本机数据库。

## 首次准备

```bash
cp .env.example .env
chmod 600 .env
```

必须填写：

- `PASSWORD`：新的强访问密码。
- `POSTGRESQL_URI`：当前服务器自己的 PostgreSQL 连接串。
- `COS_SECRET_ID`、`COS_SECRET_KEY`、`COS_REGION`、`COS_BUCKET`：需要 COS 时成组填写。
- `PRIMARY_IMAGE_TAG`、`CANDIDATE_IMAGE_TAG`、`V3_IMAGE_TAG`：使用对应代码的 Git 提交 ID。

不要把 `.env`、凭证 JSON 或 PEM 私钥放进 Git。`.dockerignore` 会阻止这些文件进入镜像。

## 校验配置

在启动前检查 Compose 展开结果：

```bash
docker compose config --quiet
docker compose config --services
```

在尚未创建真实 `.env` 时，可以用模板执行无密钥的结构检查：

```bash
ENV_FILE=.env.example \
PASSWORD=validation-only \
POSTGRESQL_URI=postgresql://validation:validation@host.docker.internal:15432/gcli2api \
docker compose config --quiet
```

production-1 应显示三个服务；production-2 应显示两个服务。

## 蓝绿发布

假设当前主实例运行旧版本，准备将当前代码部署到候选端口 `7862`：

1. 将 `.env` 中的 `CANDIDATE_IMAGE_TAG` 设置为当前 Git 提交 ID。
2. 只构建并启动候选实例：

   ```bash
   docker compose build gcli2api-candidate
   docker compose up -d --no-deps gcli2api-candidate
   docker compose ps
   ```

3. 验证候选实例：

   ```bash
   curl --fail http://127.0.0.1:7862/
   ```

4. 在 Nginx 切换流量并观察稳定后，把 `PRIMARY_IMAGE_TAG` 设置成相同提交 ID，再更新主实例：

   ```bash
   docker compose up -d --no-deps gcli2api
   ```

production-1 的 `7863` 由 `gcli2api-v3` 服务管理，发布方式相同，只需使用 `V3_IMAGE_TAG` 和对应服务名。

## 健康与回滚

所有实例都带有 Docker 健康检查和日志轮转。查看状态：

```bash
docker compose ps
docker compose logs --tail=100 gcli2api-candidate
```

回滚时，把相应的镜像标签恢复为上一个 Git 提交 ID，然后只更新对应服务。不要在服务器上使用 `git reset --hard` 覆盖未确认的生产文件。

## 网络安全

- 后端端口需要供独立 Nginx 服务器访问时，应在云安全组中仅允许 Nginx 服务器来源 IP。
- PostgreSQL `15432` 不应向全网开放。
- 控制面板和 API 不应继续使用项目默认密码。
- API 密钥应放在请求头中，不要放进 URL 查询参数或 Nginx 访问日志。
- 对外域名应配置 HTTPS。
