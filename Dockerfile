# Production image for gcli2api
FROM python:3.13-slim AS base

ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="gcli2api" \
      org.opencontainers.image.source="https://github.com/AstonChenDev/gcli2api" \
      org.opencontainers.image.revision="${VCS_REF}"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai \
    MALLOC_TRIM_THRESHOLD_=100000 \
    MALLOC_ARENA_MAX=2

# Install tzdata, jemalloc and set timezone
# jemalloc 替代 glibc malloc，大幅减少内存碎片化
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata libjemalloc2 && \
    JEMALLOC_PATH="$(find /usr/lib -name libjemalloc.so.2 -print -quit)" && \
    ln -s "$JEMALLOC_PATH" /usr/local/lib/libjemalloc.so.2 && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 使用 jemalloc 替代 glibc malloc
# jemalloc 会主动将空闲内存归还给操作系统，避免 RSS 持续增长
ENV LD_PRELOAD=/usr/local/lib/libjemalloc.so.2
# jemalloc 配置：启用后台线程回收、脏页衰减时间 5 秒（默认 10 秒）
ENV MALLOC_CONF="background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000"

WORKDIR /app

# Copy only requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Application, candidate, v3 and OAuth callback ports
EXPOSE 7861 7862 7863 11451 11454

STOPSIGNAL SIGTERM

# Default command
CMD ["python", "web.py"]
