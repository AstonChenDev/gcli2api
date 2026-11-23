"""
数据库层（SQLAlchemy 2.0 异步 ORM）
统一支持 SQLite（默认）与 PostgreSQL。
凭证以 email 为唯一键；schema 变更不做旧数据兼容（新表直接重建）。
"""

import asyncio
import json
import math
import os
import random
import ssl
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    event,
    Boolean,
    Float,
    Index,
    Integer,
    String,
    Text,
    case,
    cast,
    delete,
    func,
    inspect,
    literal,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from log import log
from src.credential_expiry import expiry_allows_dispatch
from src.credential_tier import DEFAULT_PROJECT_ID


def _asyncpg_ssl_from_sslmode(sslmode: str, sslrootcert: Optional[str] = None):
    """将 libpq 风格的 sslmode 值转换为 asyncpg 的 ssl 连接参数。

    asyncpg 的 connect() 不接受 libpq/psycopg2 的 sslmode 关键字；URI 中携带
    ?sslmode=... 时 SQLAlchemy 会将其原样透传，导致报错：
    "connect() got an unexpected keyword argument 'sslmode'"。
    因此必须把 sslmode 从查询串中摘出，转换成 ssl 参数传入。
    """
    if sslmode == "disable":
        return False
    ctx = ssl.create_default_context()
    if sslmode in ("require", "prefer", "allow"):
        # libpq 语义：加密传输但不校验服务端证书
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif sslmode == "verify-ca":
        # 校验证书链但不校验主机名
        ctx.check_hostname = False
    # verify-full：保持默认 context（校验证书链 + 主机名）
    if sslrootcert:
        ctx.load_verify_locations(cafile=sslrootcert)
    return ctx


# ====================== ORM 模型 ======================

class Base(DeclarativeBase):
    pass


class AntigravityCredential(Base):
    """Antigravity 凭证表（email 唯一键）"""
    __tablename__ = "antigravity_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)

    # 凭证结构化字段
    access_token: Mapped[str] = mapped_column(Text, default="")
    refresh_token: Mapped[str] = mapped_column(Text, default="")
    client_id: Mapped[str] = mapped_column(Text, default="")
    client_secret: Mapped[str] = mapped_column(Text, default="")
    project_id: Mapped[str] = mapped_column(Text, default="")
    expiry: Mapped[str] = mapped_column(Text, default="")

    # Existing rows start at the legacy revision; new or replaced identities get a unique value.
    credential_revision: Mapped[str] = mapped_column(String(32), default=lambda: uuid.uuid4().hex,
                                                     server_default=text("''"))
    refresh_owner: Mapped[str] = mapped_column(String(32), default="")
    refresh_until: Mapped[float] = mapped_column(Float, default=0.0)

    # 状态字段
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    error_codes: Mapped[str] = mapped_column(Text, default="[]")
    error_messages: Mapped[str] = mapped_column(Text, default="[]")
    last_success: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 模型级冷却 (JSON: {model_name: cooldown_timestamp})
    model_cooldowns: Mapped[str] = mapped_column(Text, default="{}")

    # tier 状态 (free/pro/ultra/ultra_lite)
    tier: Mapped[Optional[str]] = mapped_column(String(32), default="pro")
    note: Mapped[str] = mapped_column(Text, default="")

    # Per-credential egress; null preserves the existing global proxy policy.
    proxy_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    concurrency_limit: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    rpm_limit: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    # 时间戳
    created_at: Mapped[float] = mapped_column(Float, default=time.time)
    updated_at: Mapped[float] = mapped_column(Float, default=time.time, onupdate=time.time)

    __table_args__ = (
        Index("idx_ag_disabled", "disabled"),
    )


class ProxyEndpoint(Base):
    """User-managed outbound proxies; URLs may contain authentication secrets."""
    __tablename__ = "proxy_endpoints"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[float] = mapped_column(Float, default=time.time)


class ConfigItem(Base):
    """配置表"""
    __tablename__ = "config"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[float] = mapped_column(Float, default=time.time)


class Credential499State(Base):
    """Separate breaker state: never cleared by record_success or quota CD."""
    __tablename__ = "credential_499_state"
    email: Mapped[str] = mapped_column(String(320), primary_key=True)
    state: Mapped[str] = mapped_column(Text, default="{}")


class CredentialEntitlement(Base):
    """Shared last-known model permissions and a bounded refresh lease."""
    __tablename__ = "credential_entitlements"
    email: Mapped[str] = mapped_column(String(320), primary_key=True)
    snapshot: Mapped[str] = mapped_column(Text, default="{}")
    next_refresh: Mapped[float] = mapped_column(Float, default=0)
    lease_until: Mapped[float] = mapped_column(Float, default=0)
    lease_token: Mapped[str] = mapped_column(String(32), default="")


class UsageLog(Base):
    """请求日志表：每次 API 调用记录一条（成功与失败均记录）"""
    __tablename__ = "usage_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[float] = mapped_column(Float, default=time.time, index=True)

    # 请求来源
    api_format: Mapped[str] = mapped_column(String(16), default="")        # openai / gemini / anthropic
    client_ip: Mapped[str] = mapped_column(String(64), default="")
    request_id: Mapped[str] = mapped_column(String(64), default="")
    response_id: Mapped[str] = mapped_column(String(160), default="")

    # 调用信息
    model_name: Mapped[str] = mapped_column(String(128), default="")             # 客户端请求的模型名（原始，可能带功能前缀）
    upstream_model_name: Mapped[str] = mapped_column(String(128), default="")   # 后端实际使用的真实模型名（映射后）
    credential_email: Mapped[str] = mapped_column(String(320), default="")
    is_stream: Mapped[bool] = mapped_column(Boolean, default=False)

    # 结果
    status_code: Mapped[int] = mapped_column(Integer, default=0)
    success: Mapped[bool] = mapped_column(Boolean, default=False)
    use_time: Mapped[float] = mapped_column(Float, default=0.0)            # 耗时（秒）

    # Token 用量
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cached_tokens: Mapped[int] = mapped_column(Integer, default=0)  # prompt 中命中缓存的 token（含在 prompt_tokens 内）

    # 失败时的错误摘要
    error_message: Mapped[str] = mapped_column(Text, default="")

    # 请求体详细日志文件索引（data/request_logs 相对路径；空=未记录）
    request_log_file: Mapped[str] = mapped_column(String(512), default="")

    __table_args__ = (
        Index("idx_ul_created_model", "created_at", "model_name"),
        Index("idx_ul_created_email", "created_at", "credential_email"),
        Index("idx_ul_request_id", "request_id"),
        Index("idx_ul_response_id", "response_id"),
        Index("idx_ul_request_log_file", "request_log_file"),
    )


# ====================== JSON 序列化辅助 ======================

def _dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _load_json(raw: Optional[str], default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return default


MODEL_NOT_FOUND_TTL = 300
_MODEL_CATALOG_ALIASES = {
    "claude-sonnet-4-6-thinking": "claude-sonnet-4-6",
    "claude-opus-4-6": "claude-opus-4-6-thinking",
}


def catalog_model_key(model: str) -> str:
    name = model.strip().lower()
    return _MODEL_CATALOG_ALIASES.get(name, name)


def _catalog_model_rejected(snapshot, model, now):
    rejected = snapshot.get("model_not_found")
    since = rejected.get(catalog_model_key(model)) if isinstance(rejected, dict) else None
    return isinstance(since, (int, float)) and now - MODEL_NOT_FOUND_TTL < since <= now


def catalog_models(snapshot):
    """Return the last verified catalog minus recent model-specific 404s."""
    snapshot = _load_json(snapshot, {})
    if not isinstance(snapshot, dict):
        return None
    models = snapshot.get("models")
    if not isinstance(models, list) or not all(isinstance(model, str) for model in models):
        return None
    now = time.time()
    return [model for model in models if model and not _catalog_model_rejected(snapshot, model, now)]


def catalog_supports_model(snapshot, model, *, now=None):
    """None means unknown; failed refreshes never become an empty catalog."""
    if not model:
        return None
    data = _load_json(snapshot, {})
    if not isinstance(data, dict):
        return None
    if _catalog_model_rejected(data, model, time.time() if now is None else now):
        return False
    models = data.get("models")
    if not isinstance(models, list) or not all(isinstance(name, str) for name in models):
        return None
    key = catalog_model_key(model)
    return any(catalog_model_key(name) == key for name in models)


class UnsupportedCredentialModel(Exception):
    def __init__(self, model):
        super().__init__(f"该凭证的模型目录显示 {model} 当前不可用")


class PoolModelUnavailable(Exception):
    status_code = 404

    def __init__(self, model):
        super().__init__(f"当前所有已启用凭证的模型目录均显示 {model} 不可用")


def _row_to_credential_dict(row: AntigravityCredential) -> Dict[str, Any]:
    """ORM 行 -> 凭证字典"""
    return {
        "email": row.email,
        "credential_revision": row.credential_revision,
        "access_token": row.access_token or "",
        "refresh_token": row.refresh_token or "",
        "client_id": row.client_id or "",
        "client_secret": row.client_secret or "",
        "project_id": row.project_id or "",
        "expiry": row.expiry or "",
        "tier": row.tier or "pro",
        "note": row.note or "",
    }


def dispatch_limit_wait(concurrency_limit, rpm_limit, state, now):
    if rpm_limit:
        starts = [t for t in state.get("dispatch_starts", ()) if t > now - 60]
        if len(starts) >= rpm_limit:
            return max(1, math.ceil(sorted(starts)[-rpm_limit] + 60 - now))
    if concurrency_limit and sum(lease["expires"] > now for lease in state.get("leases", {}).values()) >= concurrency_limit:
        return 1
    return 0


def get_cooldown_group(model_name: str) -> str:
    """
    模型 CD 分组：同家族模型共享一个冷却键
    - 名字带 image -> "image"
    - 名字带 argon -> "argon"
    - 名字带 gemini -> "gemini"
    - 名字带 claude 或 gpt -> "claude_gpt"
    - 其他 -> 原始模型名
    """
    lowered = (model_name or "").lower()
    if "image" in lowered:
        return "image"
    if "argon" in lowered:
        return "argon"
    if "gemini" in lowered:
        return "gemini"
    if "claude" in lowered or "gpt" in lowered:
        return "claude_gpt"
    return model_name


# ====================== 数据库管理器 ======================

class Database:
    """统一数据库管理器（SQLite / PostgreSQL）"""

    def __init__(self):
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None
        self._initialized = False
        self._lock = asyncio.Lock()
        self._backend_type = "sqlite"

        # 内存配置缓存
        self._config_cache: Dict[str, Any] = {}
        self._config_versions: Dict[str, float] = {}
        self._config_loaded = False
        self._config_lock = asyncio.Lock()

    # ---------- 初始化 ----------

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            postgres_uri = os.getenv("POSTGRESQL_URI", "")
            credentials_dir = os.getenv("CREDENTIALS_DIR", "./creds")
            os.makedirs(credentials_dir, exist_ok=True)

            if postgres_uri:
                # PostgreSQL（postgres:// 或 postgresql:// -> postgresql+asyncpg://）
                dsn = postgres_uri
                if dsn.startswith("postgresql://"):
                    dsn = "postgresql+asyncpg://" + dsn[len("postgresql://"):]
                elif dsn.startswith("postgres://"):
                    dsn = "postgresql+asyncpg://" + dsn[len("postgres://"):]
                elif not dsn.startswith("postgresql+"):
                    dsn = "postgresql+asyncpg://" + dsn
                engine_kwargs: Dict[str, Any] = {
                    "pool_size": 5,
                    "max_overflow": 5,
                    "pool_pre_ping": True,
                }

                # asyncpg 不支持 libpq 的 sslmode 查询参数（Aiven/Neon/Supabase
                # 等云端 PostgreSQL 的 URI 普遍带 ?sslmode=require），需从 URL
                # 中摘出并转换为 connect_args 的 ssl 参数，否则会抛出
                # TypeError: connect() got an unexpected keyword argument 'sslmode'
                url = make_url(dsn)
                sslmode = url.query.get("sslmode")
                if sslmode:
                    query = dict(url.query)
                    del query["sslmode"]
                    sslrootcert = query.pop("sslrootcert", None)
                    url = url.set(query=query)
                    engine_kwargs["connect_args"] = {
                        "ssl": _asyncpg_ssl_from_sslmode(sslmode, sslrootcert)
                    }
                    log.info(f"POSTGRESQL_URI 的 sslmode={sslmode} 已转换为 asyncpg 的 ssl 连接参数")

                self._engine = create_async_engine(url, **engine_kwargs)
                self._backend_type = "postgresql"
            else:
                # SQLite 默认
                db_path = os.path.join(credentials_dir, "credentials.db")
                self._engine = create_async_engine(
                    f"sqlite+aiosqlite:///{db_path}",
                    # 连接池：多个长连接复用（默认 NullPool 每会话新建连接开销大）；
                    # WAL 模式允许读写并发，单写者由 busy_timeout 兜底
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True,
                    connect_args={"timeout": 30},
                )
                # WAL + NORMAL：读写并发 + 减少每次提交的 fsync（PRAGMA 需在连接建立后执行）

                @event.listens_for(self._engine.sync_engine, "connect")
                def _set_sqlite_pragma(dbapi_conn, conn_record):
                    cursor = dbapi_conn.cursor()
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cursor.execute("PRAGMA synchronous=NORMAL")
                    cursor.execute("PRAGMA busy_timeout=15000")
                    cursor.execute("PRAGMA cache_size=-8000")
                    cursor.close()

                self._backend_type = "sqlite"

            self._session_factory = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )

            # 建表（不兼容旧结构：旧的 filename 键结构表会被重建为 email 键结构）
            async with self._engine.begin() as conn:
                # Serialize schema inspection and changes across workers starting together.
                if self.backend_type == "sqlite":
                    await conn.execute(text("BEGIN IMMEDIATE"))
                else:
                    await conn.execute(text(
                        "SELECT pg_advisory_xact_lock(hashtext(current_database()), "
                        "hashtext(current_schema() || '.agy2api_schema'))"
                    ))
                await conn.run_sync(self._create_tables)

            # 标记初始化完成（供 session() 使用），再加载配置缓存
            self._initialized = True

            # 加载配置缓存
            await self._load_config_cache()

            log.info(f"Database initialized (backend={self._backend_type})")

    def _create_tables(self, sync_conn) -> None:
        """建表。
        - 凭证表：结构性变更（键从 filename 变为 email）时删除重建
        - 其余表：缺列时用 ALTER TABLE ADD COLUMN 补齐，保留原有数据
        """
        inspector = inspect(sync_conn)
        table_name = AntigravityCredential.__tablename__
        if inspector.has_table(table_name):
            existing_columns = {c["name"] for c in inspector.get_columns(table_name)}
            # 旧结构以 filename 为键、无 email 列 —— 删除重建（数据无法迁移）
            if "filename" in existing_columns or "email" not in existing_columns:
                log.info("检测到旧版凭证表结构（filename 键），重建为 email 键结构")
                sync_conn.execute(text(f"DROP TABLE {table_name}"))

        # 已存在的表补齐缺失列（ALTER TABLE ADD COLUMN，保留数据）
        inspector = inspect(sync_conn)
        for orm_model in (AntigravityCredential, UsageLog):
            table = orm_model.__table__
            if not inspector.has_table(table.name):
                continue
            existing_columns = {c["name"] for c in inspector.get_columns(table.name)}
            dialect = inspector.bind.dialect
            for column in table.columns:
                if column.name in existing_columns:
                    continue
                column_type = column.type.compile(dialect)
                # 补列：有 server_default 用之；有 Python default 用字面量；否则 DEFAULT NULL
                if column.server_default is not None:
                    default_clause = f" DEFAULT {column.server_default.arg}"
                elif column.default is not None and column.default.is_scalar:
                    default_clause = f" DEFAULT {column.default.arg!r}"
                else:
                    default_clause = " DEFAULT NULL"
                sync_conn.execute(
                    text(f"ALTER TABLE {table.name} ADD COLUMN {column.name} {column_type}{default_clause}")
                )
                log.info(f"表 {table.name} 补充列 {column.name} ({column_type})")

        Base.metadata.create_all(sync_conn)
        for index in UsageLog.__table__.indexes:
            index.create(sync_conn, checkfirst=True)

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
            self._engine = None
        self._session_factory = None
        self._initialized = False
        log.debug("Database closed")

    @property
    def backend_type(self) -> str:
        return self._backend_type

    def _ensure_initialized(self):
        if not self._initialized or not self._session_factory:
            raise RuntimeError("Database not initialized")

    @asynccontextmanager
    async def session(self):
        """获取数据库会话（自动提交/回滚）"""
        self._ensure_initialized()
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    # ---------- 凭证管理 ----------

    @asynccontextmanager
    async def locked_cooldown_state(self, email):
        """Common lock order: breaker row, then credential row if needed."""
        async with self.session() as session:
            if self.backend_type == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
                insert = sqlite_insert
            else:
                insert = postgresql_insert
            await session.execute(insert(Credential499State).values(email=email, state="{}")
                                  .on_conflict_do_nothing(index_elements=["email"]))
            state_row = await session.scalar(select(Credential499State).where(
                Credential499State.email == email).with_for_update())
            yield session, state_row

    async def get_import_identities(self):
        """Import-only identity index; never expose this result through an API."""
        async with self.session() as s:
            return (await s.execute(select(AntigravityCredential.email, AntigravityCredential.refresh_token))).all()

    async def insert_imported_credential(self, email, credential_data):
        """Atomic insert-only import: concurrent duplicates preserve all existing fields."""
        if self.backend_type == "postgresql":
            insert = postgresql_insert
        else:
            insert = sqlite_insert
        values = {k: credential_data[k] for k in (
            "access_token", "refresh_token", "client_id", "client_secret", "project_id", "expiry", "tier"
        )}
        stmt = insert(AntigravityCredential).values(email=email, disabled=values["tier"] == "free",
                                                   last_success=time.time(), **values)
        stmt = stmt.on_conflict_do_nothing(index_elements=["email"]).returning(AntigravityCredential.id)
        async with self.session() as s:
            return (await s.scalar(stmt)) is not None

    async def store_credential(
        self, email: str, credential_data: Dict[str, Any]
    ) -> bool:
        """存储或更新凭证（以 email 为唯一键，保留状态字段）"""
        async with self.session() as s:
            if self.backend_type == "sqlite":
                await s.execute(text("BEGIN IMMEDIATE"))
            row = await s.scalar(
                select(AntigravityCredential).where(AntigravityCredential.email == email).with_for_update()
            )
            access_token = credential_data.get("access_token") or credential_data.get("token") or ""
            refresh_token = credential_data.get("refresh_token") or ""
            client_id = credential_data.get("client_id") or ""
            client_secret = credential_data.get("client_secret") or ""
            project_id = credential_data.get("project_id") or (row.project_id if row else None)
            if not project_id:
                project_id = DEFAULT_PROJECT_ID if "project_id" in credential_data else ""
            expiry_val = credential_data.get("expiry") or ""
            expiry = expiry_val.isoformat() if hasattr(expiry_val, "isoformat") else str(expiry_val)
            tier = credential_data.get("tier")
            disable = tier == "free" and credential_data.get("verified_free") is True

            if row:
                if (row.refresh_token, row.client_id, row.client_secret, row.project_id) != (refresh_token, client_id, client_secret, project_id):
                    await self._invalidate_credential_identity(s, row)
                row.access_token = access_token
                row.refresh_token = refresh_token
                row.client_id = client_id
                row.client_secret = client_secret
                row.project_id = project_id
                row.expiry = expiry
                if "note" in credential_data:
                    row.note = credential_data["note"]
                if tier:
                    row.tier = tier
                if disable:
                    row.disabled = True
                row.updated_at = time.time()
            else:
                s.add(AntigravityCredential(
                    email=email,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    client_id=client_id,
                    client_secret=client_secret,
                    project_id=project_id,
                    expiry=expiry,
                    tier=tier or ("unknown" if "tier" in credential_data else "pro"),
                    disabled=disable,
                    note=credential_data.get("note", ""),
                    last_success=time.time(),
                ))
            log.debug(f"Stored credential: {email}")
            return True

    async def get_credential(
        self, email: str
    ) -> Optional[Dict[str, Any]]:
        """获取凭证数据"""
        async with self.session() as s:
            row = await s.scalar(
                select(AntigravityCredential).where(AntigravityCredential.email == email)
            )
            return _row_to_credential_dict(row) if row else None

    async def update_access_token(self, email, expected_refresh_token, access_token, expiry):
        """Token renewal must not overwrite a concurrently refreshed tier/note."""
        async with self.session() as s:
            result = await s.execute(update(AntigravityCredential).where(
                AntigravityCredential.email == email,
                AntigravityCredential.refresh_token == expected_refresh_token,
            ).values(access_token=access_token, expiry=expiry, updated_at=time.time()))
            return result.rowcount == 1

    async def get_credential_exports(self, emails: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """Read portable credential fields and their model catalogs in one database snapshot."""
        cred = AntigravityCredential
        query = (select(cred.email, cred.refresh_token, cred.access_token, cred.expiry,
                        cred.tier, cred.project_id, cred.note, CredentialEntitlement.snapshot)
                 .outerjoin(CredentialEntitlement, CredentialEntitlement.email == cred.email)
                 .order_by(cred.id))
        if emails is not None:
            query = query.where(cred.email.in_(emails))
        async with self.session() as session:
            rows = (await session.execute(query)).all()
        return {
            row.email: {
                "email": row.email, "refresh_token": row.refresh_token or "",
                "access_token": row.access_token or "", "expiry": row.expiry or "",
                "tier": row.tier or "pro", "project_id": row.project_id or "",
                "supported_models": catalog_models(row.snapshot) or [],
                **({"note": row.note} if row.note else {}),
            }
            for row in rows
        }

    async def list_credentials(self) -> List[str]:
        """列出所有凭证 email（创建顺序）"""
        async with self.session() as s:
            rows = await s.scalars(
                select(AntigravityCredential.email).order_by(AntigravityCredential.id)
            )
            return list(rows)

    async def delete_credential(self, email: str) -> bool:
        """删除凭证"""
        async with self.locked_cooldown_state(email) as (s, state_row):
            row = await s.scalar(
                select(AntigravityCredential).where(AntigravityCredential.email == email).with_for_update()
            )
            state = json.loads(state_row.state)
            # Keep ownership of live transports: deleting lease keys would make
            # heartbeat renewal cancel them. Removing groups fences old results
            # even when the same email is imported again before they finish.
            state.update(groups={}, seen={})
            state_row.state = _dump_json(state)
            # Retain this empty lock row even with no leases. Removing it could
            # race a worker between its INSERT ON CONFLICT and SELECT FOR UPDATE.
            if row:
                await s.execute(delete(CredentialEntitlement).where(CredentialEntitlement.email == email))
                await s.delete(row)
                return True
            return False

    async def get_enabled_model_catalog(self) -> Optional[List[str]]:
        """Union of verified model catalogs belonging to enabled credentials."""
        async with self.session() as session:
            snapshots = await session.scalars(
                select(CredentialEntitlement.snapshot)
                .join(AntigravityCredential, AntigravityCredential.email == CredentialEntitlement.email)
                .where(AntigravityCredential.disabled.is_(False))
            )
            catalogs = [models for snapshot in snapshots if (models := catalog_models(snapshot)) is not None]
            if not catalogs:
                return None
            return sorted({model for models in catalogs for model in models})

    async def all_enabled_credentials_lack_model(self, model):
        async with self.session() as session:
            snapshots = (await session.scalars(
                select(CredentialEntitlement.snapshot).select_from(AntigravityCredential)
                .outerjoin(CredentialEntitlement, CredentialEntitlement.email == AntigravityCredential.email)
                .where(AntigravityCredential.disabled.is_(False))
            )).all()
        return bool(snapshots) and all(catalog_supports_model(snapshot, model) is False for snapshot in snapshots)

    async def get_dispatch_limit_waits(self, model, *, manual_email=None):
        """Read admission metadata only; never fetch tokens or reserve a slot."""
        cred = AntigravityCredential
        query = (select(cred.email, cred.concurrency_limit, cred.rpm_limit, cred.model_cooldowns,
                        Credential499State.state, CredentialEntitlement.snapshot)
                 .outerjoin(CredentialEntitlement, CredentialEntitlement.email == cred.email)
                 .join(Credential499State, Credential499State.email == cred.email, isouter=manual_email is not None))
        if manual_email is not None:
            query = query.where(cred.email == manual_email)
        else:
            query = query.where(cred.disabled.is_(False), or_(cred.concurrency_limit > 0, cred.rpm_limit > 0))
        async with self.session() as session:
            rows = (await session.execute(query)).all()
        now = time.time()
        waits = {}
        for email, concurrency, rpm, cooldowns, state, catalog in rows:
            if catalog_supports_model(catalog, model) is False:
                if manual_email is not None:
                    raise UnsupportedCredentialModel(model)
                continue
            if not manual_email and _load_json(cooldowns, {}).get(get_cooldown_group(model), 0) > now:
                continue
            wait = dispatch_limit_wait(concurrency, rpm, json.loads(state or "{}"), now)
            if wait:
                waits[email] = wait
        return waits

    async def get_next_available_credential(
        self, model_name: Optional[str] = None, exclude_emails=None, *, require_ready_token=False,
        report_missing_model=False,
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """随机获取一个可用凭证（负载均衡 + 模型家族级冷却过滤），返回 (email, credential)"""
        current_time = time.time()
        cd_group = get_cooldown_group(model_name) if model_name else None
        async with self.session() as s:
            # 轻量查询：只取必要的小字段（email/冷却/禁用），避免全表加载
            # access_token/refresh_token 等大字段（凭证明文可达数 KB）
            stmt = (
                select(AntigravityCredential.email, AntigravityCredential.model_cooldowns, CredentialEntitlement.snapshot)
                .outerjoin(CredentialEntitlement, CredentialEntitlement.email == AntigravityCredential.email)
                .where(AntigravityCredential.disabled == False)  # noqa: E712
                .order_by(AntigravityCredential.id)
            )
            excluded = set(exclude_emails or ())
            if excluded and not report_missing_model:
                stmt = stmt.where(AntigravityCredential.email.not_in(excluded))
            if require_ready_token:
                has_token = func.trim(AntigravityCredential.access_token) != ""
                stmt = stmt.add_columns(AntigravityCredential.expiry, has_token.label("has_token"))
                if not report_missing_model:
                    stmt = stmt.where(has_token)
            rows = (await s.execute(stmt)).all()
            if not rows:
                return None
            # Python 侧随机负载均衡（过滤模型家族冷却）
            confirmed, unknown = [], []
            # For an initial generation request, classify this same set of
            # enabled rows, including accounts excluded by quota or token state.
            all_missing = report_missing_model and bool(model_name)
            now = datetime.fromtimestamp(current_time, timezone.utc)
            for candidate in rows:
                supported = catalog_supports_model(candidate.snapshot, model_name, now=current_time)
                if supported is False:
                    continue
                all_missing = False
                email, model_cooldowns = candidate.email, candidate.model_cooldowns
                if email in excluded:
                    continue
                if require_ready_token and (not candidate.has_token or not expiry_allows_dispatch(candidate.expiry, now)):
                    continue
                cooldowns = _load_json(model_cooldowns, {})
                cd = cooldowns.get(cd_group) if cd_group else None
                if cd is not None and current_time < cd:
                    continue
                if supported is True:
                    confirmed.append(email)
                else:
                    unknown.append(email)
            if not confirmed and not unknown:
                if all_missing:
                    raise PoolModelUnavailable(model_name)
                return None

            # A panel action or entitlement refresh can remove a shortlisted
            # account before its token is read. Try each eligible email at most once.
            for pool in (confirmed, unknown):
                while pool:
                    email = random.choice(pool)
                    row = await s.scalar(select(AntigravityCredential).where(
                        AntigravityCredential.email == email, AntigravityCredential.disabled.is_(False)
                    ))
                    if row is not None:
                        return row.email, _row_to_credential_dict(row)
                    pool.remove(email)

        if report_missing_model and model_name and await self.all_enabled_credentials_lack_model(model_name):
            raise PoolModelUnavailable(model_name)
        return None

    async def get_available_credential_by_email(
        self, email: str, model_name: Optional[str] = None
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        按指定 email 获取可用凭证（凭证亲和/粘性模式使用）。
        凭证不存在/被禁用/该模型家族冷却中 -> None。
        """
        if not email:
            return None
        current_time = time.time()
        cd_group = get_cooldown_group(model_name) if model_name else None
        async with self.session() as s:
            row = await s.scalar(
                select(AntigravityCredential).where(AntigravityCredential.email == email)
            )
            if not row or row.disabled:
                return None
            snapshot = await s.scalar(select(CredentialEntitlement.snapshot).where(CredentialEntitlement.email == email))
            if catalog_supports_model(snapshot, model_name) is False:
                return None
            if cd_group:
                cooldowns = _load_json(row.model_cooldowns, {})
                cd = cooldowns.get(cd_group)
                if cd is not None and current_time < cd:
                    return None
            return row.email, _row_to_credential_dict(row)

    # ---------- 状态管理 ----------

    @staticmethod
    async def _invalidate_credential_identity(session, row):
        row.credential_revision = uuid.uuid4().hex
        row.refresh_owner, row.refresh_until = "", 0
        await session.execute(delete(CredentialEntitlement).where(CredentialEntitlement.email == row.email))

    async def update_credential_state(
        self, email: str, state_updates: Dict[str, Any], *, expected_refresh_token: Optional[str] = None,
        expected_revision: Optional[str] = None, verified_tier_sequence: Optional[int] = None,
    ) -> bool:
        """更新凭证状态字段"""
        json_fields = {"error_codes", "error_messages", "model_cooldowns"}
        float_fields = {"last_success"}
        str_fields = {"user_email", "tier", "project_id", "note"}
        bool_fields = {"disabled"}
        async with self.session() as s:
            values: Dict[str, Any] = {}
            for key, value in state_updates.items():
                if key in json_fields:
                    values[key] = _dump_json(value)
                elif key in float_fields:
                    values[key] = float(value) if value is not None else None
                elif key in str_fields:
                    values[key] = value
                elif key in bool_fields:
                    values[key] = bool(value)
                elif key in {"concurrency_limit", "rpm_limit"}:
                    values[key] = value
            if not values:
                return True
            values["updated_at"] = time.time()
            conditions = [AntigravityCredential.email == email]
            if expected_revision is not None:
                conditions.append(AntigravityCredential.credential_revision == expected_revision)
            if expected_refresh_token is not None:
                conditions.append(AntigravityCredential.refresh_token == expected_refresh_token)
            if "project_id" in values:
                if self.backend_type == "sqlite":
                    await s.execute(text("BEGIN IMMEDIATE"))
                row = await s.scalar(select(AntigravityCredential).where(*conditions).with_for_update())
                if row is None:
                    return False
                if verified_tier_sequence is not None:
                    entitlement = await s.get(CredentialEntitlement, email, with_for_update=True)
                    if entitlement is None:
                        return False
                    snapshot = json.loads(entitlement.snapshot)
                    if verified_tier_sequence < snapshot.get("applied_tier_check", 0):
                        return False
                    snapshot.update(applied_tier_check=verified_tier_sequence, tier=values["tier"],
                                    tier_checked_at=values["updated_at"])
                    entitlement.snapshot = _dump_json(snapshot)
                if row.project_id != values["project_id"]:
                    await self._invalidate_credential_identity(s, row)
                for key, value in values.items():
                    setattr(row, key, value)
                return True
            result = await s.execute(update(AntigravityCredential).where(*conditions).values(**values))
            return result.rowcount > 0

    async def get_credential_state(
        self, email: str
    ) -> Dict[str, Any]:
        """获取凭证状态（不含 error_messages）"""
        async with self.session() as s:
            match = (await s.execute(
                select(AntigravityCredential, CredentialEntitlement.snapshot)
                .outerjoin(CredentialEntitlement, CredentialEntitlement.email == AntigravityCredential.email)
                .where(AntigravityCredential.email == email)
            )).one_or_none()
            if match is None:
                return {}
            row, snapshot = match
            return {
                "supported_models": catalog_models(snapshot) or [],
                "disabled": bool(row.disabled),
                "error_codes": _load_json(row.error_codes, []),
                "last_success": row.last_success or time.time(),
                "email": row.email,
                "model_cooldowns": _load_json(row.model_cooldowns, {}),
                "tier": row.tier or "pro",
                "note": row.note or "",
                "project_id": row.project_id or "",
            }

    async def get_all_credential_states(self) -> Dict[str, Dict[str, Any]]:
        """获取所有凭证状态（自动过滤过期CD）"""
        current_time = time.time()
        async with self.session() as s:
            rows = (await s.scalars(select(AntigravityCredential))).all()
            states = {}
            for row in rows:
                cooldowns = _load_json(row.model_cooldowns, {})
                cooldowns = {k: v for k, v in cooldowns.items() if v > current_time}
                states[row.email] = {
                    "disabled": bool(row.disabled),
                    "error_codes": _load_json(row.error_codes, []),
                    "last_success": row.last_success or time.time(),
                    "email": row.email,
                    "model_cooldowns": cooldowns,
                    "tier": row.tier or "pro",
                    "note": row.note or "",
                    "project_id": row.project_id or "",
                    "proxy_id": row.proxy_id,
                    "concurrency_limit": row.concurrency_limit,
                    "rpm_limit": row.rpm_limit,
                }
            return states

    async def get_credentials_summary(
        self,
        offset: int = 0,
        limit: Optional[int] = None,
        status_filter: str = "all",
        error_code_filter: Optional[str] = None,
        cooldown_filter: Optional[str] = None,
        tier_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """凭证摘要（分页 + 状态筛选，内存过滤）"""
        current_time = time.time()
        async with self.session() as s:
            total = await s.scalar(select(func.count()).select_from(AntigravityCredential)) or 0
            disabled_count = await s.scalar(
                select(func.count()).select_from(AntigravityCredential)
                .where(AntigravityCredential.disabled == True)  # noqa: E712
            ) or 0

            stmt = select(AntigravityCredential).order_by(AntigravityCredential.id)
            if status_filter == "enabled":
                stmt = stmt.where(AntigravityCredential.disabled == False)  # noqa: E712
            elif status_filter == "disabled":
                stmt = stmt.where(AntigravityCredential.disabled == True)  # noqa: E712
            rows = (await s.scalars(stmt)).all()

            filter_value = None
            filter_int = None
            filter_none = False
            if error_code_filter and error_code_filter.strip().lower() != "all":
                if error_code_filter.strip().lower() == "none":
                    filter_none = True
                else:
                    filter_value = error_code_filter.strip()
                    try:
                        filter_int = int(filter_value)
                    except ValueError:
                        filter_int = None

            summaries = []
            for row in rows:
                error_codes = _load_json(row.error_codes, [])
                cooldowns = _load_json(row.model_cooldowns, {})
                active_cooldowns = {k: v for k, v in cooldowns.items() if v > current_time}

                if filter_none and error_codes:
                    continue
                if filter_value is not None:
                    match = False
                    for code in error_codes:
                        if code == filter_value or code == filter_int:
                            match = True
                            break
                        if isinstance(code, str) and filter_int is not None:
                            try:
                                if int(code) == filter_int:
                                    match = True
                                    break
                            except ValueError:
                                pass
                    if not match:
                        continue

                summary = {
                    "email": row.email,
                    "disabled": bool(row.disabled),
                    "error_codes": error_codes,
                    "last_success": row.last_success or current_time,
                    "model_cooldowns": active_cooldowns,
                    "tier": row.tier or "pro",
                    "note": row.note or "",
                    "project_id": row.project_id or "",
                    "proxy_id": row.proxy_id,
                    "concurrency_limit": row.concurrency_limit,
                    "rpm_limit": row.rpm_limit,
                }
                if tier_filter and tier_filter in ("free", "pro", "ultra", "ultra_lite"):
                    allowed_tiers = {"ultra", "ultra_lite"} if tier_filter == "ultra" else {tier_filter}
                    if summary["tier"] not in allowed_tiers:
                        continue
                if cooldown_filter == "in_cooldown" and not active_cooldowns:
                    continue
                if cooldown_filter == "no_cooldown" and active_cooldowns:
                    continue
                summaries.append(summary)

            total_count = len(summaries)
            page_items = summaries[offset:offset + limit] if limit is not None else summaries[offset:]
            return {
                "items": page_items,
                "total": total_count,
                "offset": offset,
                "limit": limit,
                "stats": {
                    "total": total,
                    "normal": total - disabled_count,
                    "disabled": disabled_count,
                },
            }

    async def get_duplicate_credentials_by_email(self) -> Dict[str, Any]:
        """按邮箱分组的重复凭证（email 即唯一键，此结构保留用于展示统计）"""
        async with self.session() as s:
            rows = (await s.scalars(select(AntigravityCredential).order_by(AntigravityCredential.id))).all()
            emails = [row.email for row in rows]
            return {
                "email_groups": {},
                "duplicate_groups": [],
                "duplicate_count": 0,
                "no_email_files": [],
                "no_email_count": 0,
                "unique_email_count": len(emails),
                "total_count": len(rows),
            }

    async def get_credential_errors(self, email: str) -> Dict[str, Any]:
        """获取凭证的错误信息"""
        async with self.session() as s:
            row = await s.scalar(
                select(AntigravityCredential).where(AntigravityCredential.email == email)
            )
            if row:
                return {
                    "email": email,
                    "error_codes": _load_json(row.error_codes, []),
                    "error_messages": _load_json(row.error_messages, []),
                }
            return {"email": email, "error_codes": [], "error_messages": []}

    # ---------- 模型级冷却 ----------

    @asynccontextmanager
    async def _locked_credential_update(self, email, expected_revision=None):
        """Serialize quota read/modify/write with manual cooldown resets."""
        async with self.session() as session:
            if self.backend_type == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            row = await session.scalar(select(AntigravityCredential).where(
                AntigravityCredential.email == email).with_for_update())
            if row and expected_revision is not None and row.credential_revision != expected_revision:
                row = None
            yield row

    async def set_model_cooldown(
        self,
        email: str,
        model_name: str,
        cooldown_until: Optional[float],
        *, expected_revision=None,
    ) -> bool:
        """设置/清除模型的冷却时间（按家族分组：image / argon / gemini / claude_gpt / 其他原名）"""
        cd_group = get_cooldown_group(model_name)
        async with self._locked_credential_update(email, expected_revision) as row:
            if not row:
                return False
            cooldowns = _load_json(row.model_cooldowns, {})
            if cooldown_until is None:
                cooldowns.pop(cd_group, None)
            else:
                # Concurrent upstream requests can report different reset windows.
                cooldowns[cd_group] = max(cooldowns.get(cd_group, 0), cooldown_until)
            row.model_cooldowns = _dump_json(cooldowns)
            row.updated_at = time.time()
            return True

    async def clear_all_model_cooldowns(self, email: str) -> bool:
        """清除某凭证的所有模型冷却"""
        async with self._locked_credential_update(email) as row:
            if not row:
                return False
            row.model_cooldowns = "{}"
            row.updated_at = time.time()
            return True

    async def record_success(
        self, email: str, model_name: Optional[str] = None, *, expected_revision=None
    ) -> None:
        """Clear resolved errors and expired quota state after a generation succeeds."""
        async with self._locked_credential_update(email, expected_revision) as row:
            if not row or row.disabled:
                return
            now = time.time()
            cd_group = get_cooldown_group(model_name) if model_name else None
            cooldowns = _load_json(row.model_cooldowns, {})
            # A request admitted before another request's 429 may finish later.
            if cooldowns.get(cd_group, 0) > now:
                return
            if _load_json(row.error_codes, []):
                row.error_codes = "[]"
                row.error_messages = "{}"
                row.last_success = now
                row.updated_at = now
            if cd_group in cooldowns:
                del cooldowns[cd_group]
                row.model_cooldowns = _dump_json(cooldowns)
                row.updated_at = now

    # ---------- 配置管理（内存缓存） ----------

    @staticmethod
    async def _config_snapshot(session):
        rows = await session.execute(select(ConfigItem.key, ConfigItem.value, ConfigItem.updated_at))
        values, versions = {}, {}
        for key, value, updated_at in rows:
            values[key] = _load_json(value, value)
            versions[key] = updated_at
        return values, versions

    async def _load_config_cache(self, *, only_if_changed=False) -> None:
        async with self._config_lock:
            async with self.session() as session:
                if only_if_changed and self._config_loaded:
                    rows = await session.execute(select(ConfigItem.key, ConfigItem.updated_at))
                    if dict(rows.all()) == self._config_versions:
                        return
                values, versions = await self._config_snapshot(session)
            self._config_cache, self._config_versions = values, versions
            self._config_loaded = True
            log.debug(f"Loaded {len(self._config_cache)} config items")

    async def reload_config_cache(self, *, only_if_changed=False) -> None:
        self._ensure_initialized()
        await self._load_config_cache(only_if_changed=only_if_changed)

    @asynccontextmanager
    async def config_write_session(self):
        """Serialize configuration edits and publish their snapshot only after commit."""
        async with self._config_lock:
            async with self.session() as session:
                if self.backend_type == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                else:
                    await session.execute(text("SELECT pg_advisory_xact_lock(4997861)"))
                yield session
                await session.flush()
                values, versions = await self._config_snapshot(session)
            self._config_cache, self._config_versions = values, versions

    @staticmethod
    async def write_config_values(session, values: Dict[str, Any]) -> None:
        for key, value in values.items():
            row = await session.get(ConfigItem, key)
            if row is None:
                session.add(ConfigItem(key=key, value=_dump_json(value)))
            else:
                row.value = _dump_json(value)
                # Peer workers also use this timestamp as the row's change marker.
                row.updated_at = max(time.time(), math.nextafter(row.updated_at, math.inf))

    async def set_config(self, key: str, value: Any) -> bool:
        async with self.config_write_session() as session:
            await self.write_config_values(session, {key: value})
        return True

    async def get_config(self, key: str, default: Any = None) -> Any:
        self._ensure_initialized()
        return self._config_cache.get(key, default)

    async def get_all_config(self) -> Dict[str, Any]:
        self._ensure_initialized()
        return self._config_cache.copy()

    async def delete_config(self, key: str) -> bool:
        async with self.config_write_session() as s:
            item = await s.get(ConfigItem, key)
            if item:
                await s.delete(item)
                return True
            return False

    # ---------- 请求日志（usage_log） ----------

    async def insert_usage_log(self, entry: Dict[str, Any]) -> bool:
        """写入一条请求日志（失败仅告警，不影响主流程）"""
        try:
            async with self.session() as s:
                s.add(UsageLog(
                    created_at=entry.get("created_at") or time.time(),
                    api_format=str(entry.get("api_format", "")),
                    client_ip=str(entry.get("client_ip", "")),
                    request_id=str(entry.get("request_id", "")),
                    response_id=str(entry.get("response_id", ""))[:160],
                    model_name=str(entry.get("model_name", "")),
                    upstream_model_name=str(entry.get("upstream_model_name", "")),
                    credential_email=str(entry.get("credential_email", "")),
                    is_stream=bool(entry.get("is_stream", False)),
                    status_code=int(entry.get("status_code", 0) or 0),
                    success=bool(entry.get("success", False)),
                    use_time=float(entry.get("use_time", 0.0) or 0.0),
                    prompt_tokens=int(entry.get("prompt_tokens", 0) or 0),
                    completion_tokens=int(entry.get("completion_tokens", 0) or 0),
                    total_tokens=int(entry.get("total_tokens", 0) or 0),
                    cached_tokens=int(entry.get("cached_tokens", 0) or 0),
                    error_message=str(entry.get("error_message", ""))[:2000],
                    request_log_file=str(entry.get("request_log_file", ""))[:512],
                ))
                return True
        except SQLAlchemyError as e:
            log.warning(f"写入请求日志失败: {e}")
            return False

    async def query_usage_logs(
        self,
        offset: int = 0,
        limit: int = 50,
        start_timestamp: Optional[float] = None,
        end_timestamp: Optional[float] = None,
        model_name: str = "",
        credential_email: str = "",
        api_format: str = "",
        status: str = "all",  # all / success / error
        is_stream: Optional[bool] = None,
        status_code: Optional[int] = None,
        identifier: str = "",
        id_type: str = "auto",
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页查询请求日志，返回 (日志列表, 总数)，新 -> 旧"""
        stmt = select(UsageLog)
        count_stmt = select(func.count()).select_from(UsageLog)

        if identifier:
            matches = []
            if id_type in ("auto", "request_id"):
                matches.append(UsageLog.request_id == identifier)
            if id_type in ("auto", "response_id"):
                matches.append(UsageLog.response_id == identifier)
            if id_type in ("auto", "log_id") and identifier.isascii() and identifier.isdecimal() and len(identifier) <= 19 and int(identifier) <= 9223372036854775807:
                matches.append(UsageLog.id == int(identifier))
            condition = or_(*matches) if matches else UsageLog.id == -1
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        if start_timestamp:
            stmt = stmt.where(UsageLog.created_at >= start_timestamp)
            count_stmt = count_stmt.where(UsageLog.created_at >= start_timestamp)
        if end_timestamp:
            stmt = stmt.where(UsageLog.created_at <= end_timestamp)
            count_stmt = count_stmt.where(UsageLog.created_at <= end_timestamp)
        if model_name:
            stmt = stmt.where(UsageLog.model_name == model_name)
            count_stmt = count_stmt.where(UsageLog.model_name == model_name)
        if credential_email:
            stmt = stmt.where(UsageLog.credential_email == credential_email)
            count_stmt = count_stmt.where(UsageLog.credential_email == credential_email)
        if api_format:
            stmt = stmt.where(UsageLog.api_format == api_format)
            count_stmt = count_stmt.where(UsageLog.api_format == api_format)
        if status_code is not None:
            stmt = stmt.where(UsageLog.status_code == status_code)
            count_stmt = count_stmt.where(UsageLog.status_code == status_code)
        if status == "success":
            stmt = stmt.where(UsageLog.success.is_(True))
            count_stmt = count_stmt.where(UsageLog.success.is_(True))
        elif status == "error":
            stmt = stmt.where(UsageLog.success.is_(False))
            count_stmt = count_stmt.where(UsageLog.success.is_(False))
        if is_stream is not None:
            stmt = stmt.where(UsageLog.is_stream.is_(is_stream))
            count_stmt = count_stmt.where(UsageLog.is_stream.is_(is_stream))

        async with self.session() as s:
            total = (await s.execute(count_stmt)).scalar() or 0
            rows = (
                (
                    await s.execute(
                        stmt.order_by(UsageLog.created_at.desc(), UsageLog.id.desc())
                        .offset(offset)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )

        items = [
            {
                "id": r.id,
                "created_at": r.created_at,
                "api_format": r.api_format or "",
                "client_ip": r.client_ip or "",
                "request_id": r.request_id or "",
                "response_id": r.response_id or "",
                "model_name": r.model_name or "",
                "upstream_model_name": r.upstream_model_name or "",
                "credential_email": r.credential_email or "",
                "is_stream": r.is_stream,
                "status_code": r.status_code,
                "success": r.success,
                "use_time": r.use_time,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "total_tokens": r.total_tokens,
                "cached_tokens": r.cached_tokens or 0,
                "error_message": r.error_message or "",
                "request_log_file": r.request_log_file or "",
            }
            for r in rows
        ]
        return items, total

    async def usage_log_stats(
        self,
        start_timestamp: Optional[float] = None,
        end_timestamp: Optional[float] = None,
        model_name: str = "",
        credential_email: str = "",
        api_format: str = "",
    ) -> Dict[str, Any]:
        """聚合统计：总请求数 / 成功数 / 失败数 / token 汇总 / 平均耗时 / 每模型聚合"""
        conditions = []
        if start_timestamp:
            conditions.append(UsageLog.created_at >= start_timestamp)
        if end_timestamp:
            conditions.append(UsageLog.created_at <= end_timestamp)
        if model_name:
            conditions.append(UsageLog.model_name == model_name)
        if credential_email:
            conditions.append(UsageLog.credential_email == credential_email)
        if api_format:
            conditions.append(UsageLog.api_format == api_format)

        success_cond = func.cast(UsageLog.success, Integer).label("success_int")

        agg = select(
            func.count().label("total_requests"),
            func.coalesce(func.sum(success_cond), 0).label("success_count"),
            func.coalesce(func.sum(UsageLog.prompt_tokens), 0).label("prompt_tokens"),
            func.coalesce(func.sum(UsageLog.completion_tokens), 0).label("completion_tokens"),
            func.coalesce(func.sum(UsageLog.total_tokens), 0).label("total_tokens"),
            func.coalesce(func.sum(UsageLog.cached_tokens), 0).label("cached_tokens"),
            func.coalesce(func.avg(UsageLog.use_time), 0).label("avg_use_time"),
        ).where(*conditions)

        by_model = (
            select(
                UsageLog.model_name.label("model_name"),
                func.count().label("count"),
                func.coalesce(func.sum(UsageLog.total_tokens), 0).label("total_tokens"),
            )
            .where(*conditions)
            .group_by(UsageLog.model_name)
            .order_by(func.count().desc())
            .limit(50)
        )

        async with self.session() as s:
            row = (await s.execute(agg)).one()
            model_rows = (await s.execute(by_model)).all()

        return {
            "total_requests": row.total_requests or 0,
            "success_count": row.success_count or 0,
            "error_count": (row.total_requests or 0) - (row.success_count or 0),
            "prompt_tokens": row.prompt_tokens or 0,
            "completion_tokens": row.completion_tokens or 0,
            "total_tokens": row.total_tokens or 0,
            "cached_tokens": row.cached_tokens or 0,
            "avg_use_time": round(float(row.avg_use_time or 0), 2),
            "by_model": [
                {"model_name": m.model_name or "(unknown)", "count": m.count, "total_tokens": m.total_tokens}
                for m in model_rows
            ],
        }

    def _credential_usage_filters(self, start_timestamp=None, end_timestamp=None, model_name="", credential_email=None):
        email = func.coalesce(UsageLog.credential_email, "")
        model = func.coalesce(func.nullif(UsageLog.upstream_model_name, ""), func.nullif(UsageLog.model_name, ""), "(unknown)")
        conditions = []
        if start_timestamp is not None:
            conditions.append(UsageLog.created_at >= start_timestamp)
        if end_timestamp is not None:
            conditions.append(UsageLog.created_at <= end_timestamp)
        if model_name:
            conditions.append(model == model_name)
        if credential_email is not None:
            conditions.append(email == credential_email)
        return email, model, conditions

    @asynccontextmanager
    async def _usage_snapshot(self):
        """Keep totals, pages and error groups on the same database snapshot."""
        async with self.session() as session:
            if self.backend_type == "postgresql":
                # READ COMMITTED takes a new snapshot for each SELECT.
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            else:
                # sqlite3's legacy transaction mode does not BEGIN for SELECT.
                await session.execute(text("BEGIN"))
            yield session

    async def model_token_stats(self, **filters):
        """Sum recorded consumption across all attempts, including failed retries.

        Do not deduplicate request IDs: each upstream attempt can consume tokens.
        Cache is a subset of input and thinking is already included in output.
        No historical per-call quota charges are stored, so never infer them here.
        """
        _, model, conditions = self._credential_usage_filters(**filters)
        prompt = func.coalesce(UsageLog.prompt_tokens, 0)
        completion = func.coalesce(UsageLog.completion_tokens, 0)
        components = prompt + completion
        total = case((UsageLog.total_tokens > 0, UsageLog.total_tokens), else_=components)
        query = select(
            model.label("model_name"), func.count().label("attempts"),
            func.sum(case((UsageLog.success.is_(True), 1), else_=0)).label("success_count"),
            func.sum(prompt).label("prompt_tokens"),
            func.sum(completion).label("completion_tokens"),
            func.sum(func.coalesce(UsageLog.cached_tokens, 0)).label("cached_tokens"),
            func.sum(total).label("total_tokens"),
            func.sum(total - components).label("total_difference_tokens"),
            func.sum(case((UsageLog.success.is_not(True), total), else_=0)).label("failed_tokens"),
            func.sum(case((total > 0, 1), else_=0)).label("positive_usage_attempts"),
            func.min(UsageLog.created_at).label("first_request"),
            func.max(UsageLog.created_at).label("last_request"),
        ).where(*conditions).group_by(model).order_by(func.sum(total).desc(), model)
        async with self._usage_snapshot() as s:
            rows = [dict(r) for r in (await s.execute(query)).mappings()]
        fields = ("attempts", "success_count", "prompt_tokens", "completion_tokens",
                  "cached_tokens", "total_tokens", "total_difference_tokens", "failed_tokens", "positive_usage_attempts")
        summary = {key: sum(row[key] for row in rows) for key in fields}
        summary["first_request"] = min((r["first_request"] for r in rows), default=None)
        summary["last_request"] = max((r["last_request"] for r in rows), default=None)
        for row in rows:
            for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
                row[field + "_percentage"] = (
                    row[field] / summary[field] * 100 if summary[field] else None)
            row["quota_percentage"] = None
        return {"summary": summary, "items": rows,
                "quota": {"available": False, "reason": "no_per_call_quota_charges"},
                "token_basis": "reported_total_with_input_plus_output_fallback",
                "scope": "all_recorded_attempts"}

    async def credential_usage_stats(self, offset=0, limit=50, **filters):
        """Aggregate retained request logs by credential and actual upstream model."""
        email, model, conditions = self._credential_usage_filters(**filters)
        succeeded = case((UsageLog.success.is_(True), 1), else_=0)
        grouped = select(
            email.label("credential_email"), model.label("model_name"),
            func.count().label("total_requests"), func.sum(succeeded).label("success_count"),
            func.max(UsageLog.created_at).label("last_request"),
            func.coalesce(func.sum(UsageLog.prompt_tokens), 0).label("prompt_tokens"),
            func.coalesce(func.sum(UsageLog.completion_tokens), 0).label("completion_tokens"),
            func.coalesce(func.sum(UsageLog.cached_tokens), 0).label("cached_tokens"),
        ).where(*conditions).group_by(email, model)
        summary_query = select(func.count(), func.coalesce(func.sum(succeeded), 0),
            func.min(UsageLog.created_at), func.max(UsageLog.created_at),
            func.coalesce(func.sum(UsageLog.prompt_tokens), 0),
            func.coalesce(func.sum(UsageLog.completion_tokens), 0),
            func.coalesce(func.sum(UsageLog.cached_tokens), 0)).where(*conditions)
        async with self._usage_snapshot() as s:
            total = await s.scalar(select(func.count()).select_from(grouped.subquery()))
            summary = (await s.execute(summary_query)).one()
            rows = (await s.execute(grouped.order_by(func.count().desc(), email, model).offset(offset).limit(limit))).all()
            errors = []
            tiers = {}
            notes = {}
            if rows:
                # Enrich after aggregation: deleted credentials must retain their log counts.
                credential_rows = (await s.execute(select(AntigravityCredential.email, AntigravityCredential.tier, AntigravityCredential.note)
                    .where(AntigravityCredential.email.in_({r.credential_email for r in rows if r.credential_email})))).all()
                tiers = {e: t.strip().lower() if t and t.strip().lower() in {"ultra", "ultra_lite", "pro", "free"} else None
                    for e, t, _ in credential_rows}
                notes = {e: note or "" for e, _, note in credential_rows}
                errors = (await s.execute(select(email, model, UsageLog.status_code, func.count())
                    .where(*conditions, UsageLog.success.is_not(True), tuple_(email, model).in_([(r.credential_email, r.model_name) for r in rows]))
                    .group_by(email, model, UsageLog.status_code).order_by(UsageLog.status_code))).all()
        error_map = {}
        for e, m, code, count in errors:
            error_map.setdefault((e, m), []).append({"status_code": code or 0, "count": count})
        return {"total": total, "summary": {
            "total_requests": summary[0], "success_count": summary[1], "failure_count": summary[0] - summary[1],
            "first_request": summary[2], "last_request": summary[3],
            "prompt_tokens": summary[4], "completion_tokens": summary[5], "cached_tokens": summary[6],
        }, "items": [{"credential_email": r.credential_email, "model_name": r.model_name,
            "credential_tier": tiers.get(r.credential_email),
            "credential_note": notes.get(r.credential_email, ""),
            "prompt_tokens": r.prompt_tokens, "completion_tokens": r.completion_tokens, "cached_tokens": r.cached_tokens,
            "total_requests": r.total_requests, "success_count": r.success_count,
            "failure_count": r.total_requests - r.success_count, "last_request": r.last_request,
            "errors": error_map.get((r.credential_email, r.model_name), [])} for r in rows]}

    async def credential_usage_errors(self, offset=0, limit=20, status_code=None, **filters):
        """Paginate distinct failure messages, so no error types are silently dropped."""
        _, _, conditions = self._credential_usage_filters(**filters)
        conditions.append(UsageLog.success.is_not(True))
        if status_code is not None:
            conditions.append(UsageLog.status_code == status_code)
        message = func.coalesce(UsageLog.error_message, "")
        grouped = select(UsageLog.status_code.label("status_code"), message.label("error_message"),
            func.count().label("count"), func.max(UsageLog.created_at).label("last_seen"))
        grouped = grouped.where(*conditions).group_by(UsageLog.status_code, message)
        async with self._usage_snapshot() as s:
            total = await s.scalar(select(func.count()).select_from(grouped.subquery()))
            rows = (await s.execute(grouped.order_by(func.count().desc(), UsageLog.status_code, message).offset(offset).limit(limit))).all()
        return {"total": total, "items": [dict(r._mapping) for r in rows]}

    def _overall_usage_source(self, scope="requests", **filters):
        """Apply time/model filters after choosing each request's latest retained result."""
        _, model, conditions = self._credential_usage_filters(**filters)
        query = select(
            UsageLog.id, model.label("model_name"), UsageLog.success.is_(True).label("success"),
            func.coalesce(UsageLog.status_code, 0).label("status_code"),
            func.coalesce(UsageLog.error_message, "").label("error_message"),
            UsageLog.created_at,
        )
        if scope == "requests":
            # Empty legacy IDs are independent records, not one giant request.
            key = case((func.coalesce(UsageLog.request_id, "") != "",
                        literal("request:") + UsageLog.request_id),
                       else_=literal("log:") + cast(UsageLog.id, String))
            ranked = select(UsageLog.id.label("log_id"),
                func.row_number().over(partition_by=key, order_by=UsageLog.id.desc()).label("position")).subquery()
            query = query.join(ranked, ranked.c.log_id == UsageLog.id).where(ranked.c.position == 1)
        elif scope != "attempts":
            raise ValueError("Unknown statistics scope")
        return query.where(*conditions).subquery()

    async def overall_usage_stats(self, **filters):
        result = {}
        async with self._usage_snapshot() as s:
            for scope in ("requests", "attempts"):
                source = self._overall_usage_source(scope, **filters)
                rows = (await s.execute(select(source.c.model_name, source.c.success,
                    source.c.status_code, func.count().label("count"))
                    .group_by(source.c.model_name, source.c.success, source.c.status_code))).all()
                models, errors = {}, {}
                for row in rows:
                    model = models.setdefault(row.model_name, {"model_name": row.model_name,
                        "total_requests": 0, "success_count": 0, "failure_count": 0})
                    model["total_requests"] += row.count
                    model["success_count" if row.success else "failure_count"] += row.count
                    if not row.success:
                        errors[row.status_code] = errors.get(row.status_code, 0) + row.count
                summary = {key: sum(m[key] for m in models.values())
                           for key in ("total_requests", "success_count", "failure_count")}
                for item in [summary, *models.values()]:
                    item["success_rate"] = (round(item["success_count"] / item["total_requests"] * 100, 2)
                                            if item["total_requests"] else None)
                result[scope] = {"summary": summary,
                    "by_model": sorted(models.values(), key=lambda m: (-m["total_requests"], m["model_name"])),
                    "error_codes": [{"status_code": code, "count": count,
                        "failure_percentage": round(count / summary["failure_count"] * 100, 2),
                        "total_percentage": round(count / summary["total_requests"] * 100, 2)}
                        for code, count in sorted(errors.items(), key=lambda item: (-item[1], item[0]))]}
        return result

    async def overall_usage_errors(self, scope="requests", offset=0, limit=20, status_code=None, **filters):
        source = self._overall_usage_source(scope, **filters)
        query = select(source.c.status_code, source.c.error_message, func.count().label("count"),
                       func.max(source.c.created_at).label("last_seen")).where(source.c.success.is_(False))
        if status_code is not None:
            query = query.where(source.c.status_code == status_code)
        query = query.group_by(source.c.status_code, source.c.error_message)
        async with self._usage_snapshot() as s:
            total = await s.scalar(select(func.count()).select_from(query.subquery()))
            rows = (await s.execute(query.order_by(func.count().desc(), source.c.status_code,
                source.c.error_message).offset(offset).limit(limit))).all()
        return {"total": total, "items": [dict(row._mapping) for row in rows]}

    async def has_request_body_log(self, path: str) -> bool:
        async with self.session() as s:
            return (await s.scalar(select(UsageLog.id).where(
                UsageLog.request_log_file == path).limit(1))) is not None

    async def delete_usage_logs_with_files(self, before_timestamp: float):
        """Atomically delete old rows and return only their detailed-log paths.

        DELETE RETURNING uses one snapshot, without dropping the table or
        racing a separate SELECT against concurrent log writers.
        """
        async with self.session() as s:
            result = await s.execute(
                delete(UsageLog).where(UsageLog.created_at < before_timestamp)
                .returning(UsageLog.request_log_file)
            )
            paths = list(result.scalars())
        # The commit must succeed before any caller removes files.
        return len(paths), [path for path in paths if path]

    async def delete_usage_logs_before(self, before_timestamp: float) -> int:
        """删除指定时间之前的日志，返回删除条数"""
        async with self.session() as s:
            result = await s.execute(
                text("DELETE FROM usage_log WHERE created_at < :ts"),
                {"ts": before_timestamp},
            )
            return result.rowcount or 0

    # ---------- 后端信息 ----------

    def get_backend_info(self) -> Dict[str, Any]:
        return {
            "backend_type": self._backend_type,
            "initialized": self._initialized,
            "database_path": (
                os.path.join(os.getenv("CREDENTIALS_DIR", "./creds"), "credentials.db")
                if self._backend_type == "sqlite"
                else None
            ),
            "credentials_dir": os.getenv("CREDENTIALS_DIR", "./creds"),
        }


# ====================== 全局单例 ======================

_database: Optional[Database] = None
_database_lock = asyncio.Lock()


async def get_database() -> Database:
    """获取全局数据库实例"""
    global _database
    if _database is None:
        async with _database_lock:
            if _database is None:
                db = Database()
                await db.initialize()
                _database = db
    return _database


async def close_database() -> None:
    """关闭全局数据库"""
    global _database
    if _database:
        await _database.close()
        _database = None
