"""
凭证请求统计收集器
内存计数器 + 定时批量刷入 PostgreSQL
"""

import asyncio
import os
import time as _time
from typing import Dict, Optional, Tuple

from log import log


class StatsCollector:
    """
    高性能统计收集器

    - 请求时仅做纯内存 dict 累加（~0.001ms，零 IO）
    - 每 FLUSH_INTERVAL 秒做一次批量 INSERT...ON CONFLICT UPDATE
    - 进程退出时 graceful shutdown 做最终刷盘
    - 分布式多 Worker 安全：SQL 用 += 原子增量
    """

    FLUSH_INTERVAL = 30  # 秒

    def __init__(self):
        # key: (filename, model_name, mode)
        # value: {"total": int, "success": int, "fail": int}
        self._counters: Dict[Tuple[str, str, str], Dict[str, int]] = {}
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

    def record(
        self,
        filename: str,
        model_name: Optional[str],
        mode: str,
        success: bool,
    ) -> None:
        """
        记录一次请求 — 纯内存操作，无 IO，线程安全（GIL + asyncio 单线程）

        Args:
            filename: 凭证文件名
            model_name: 模型名（如 gemini-2.5-flash）
            mode: geminicli 或 antigravity
            success: 是否成功
        """
        if not filename:
            return

        # 规范化
        filename = os.path.basename(filename)
        model_name = model_name or "unknown"
        key = (filename, model_name, mode)

        entry = self._counters.get(key)
        if entry is None:
            entry = {"total": 0, "success": 0, "fail": 0}
            self._counters[key] = entry

        entry["total"] += 1
        if success:
            entry["success"] += 1
        else:
            entry["fail"] += 1

    async def flush(self) -> None:
        """
        原子交换计数器 + 批量写入 DB

        使用 `data, self._counters = self._counters, {}` 原子交换，
        保证 flush 期间新请求写入新的空 dict，不丢数据。
        """
        # 原子交换
        data, self._counters = self._counters, {}

        if not data:
            return

        # 计算当前分钟的 bucket（精确到分钟的 epoch）
        bucket = int(_time.time()) // 60 * 60

        # 转为 batch 记录: [(filename, model_name, mode, total, success, fail, time_bucket), ...]
        records = [
            (key[0], key[1], key[2], vals["total"], vals["success"], vals["fail"], bucket)
            for key, vals in data.items()
        ]

        try:
            from src.storage_adapter import get_storage_adapter
            adapter = await get_storage_adapter()
            if hasattr(adapter, '_backend') and hasattr(adapter._backend, 'batch_upsert_stats'):
                await adapter._backend.batch_upsert_stats(records)
            else:
                log.warning("[STATS] Storage backend does not support batch_upsert_stats")
        except Exception as e:
            log.error(f"[STATS] Flush failed: {e}")
            # 失败时把数据合并回去，避免丢失
            for key, vals in data.items():
                entry = self._counters.get(key)
                if entry is None:
                    self._counters[key] = vals
                else:
                    entry["total"] += vals["total"]
                    entry["success"] += vals["success"]
                    entry["fail"] += vals["fail"]

    async def _flush_loop(self) -> None:
        """定时刷盘循环"""
        while self._running:
            try:
                await asyncio.sleep(self.FLUSH_INTERVAL)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[STATS] Flush loop error: {e}")

    async def start(self) -> None:
        """启动定时刷盘任务"""
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        log.info(f"[STATS] StatsCollector started (flush every {self.FLUSH_INTERVAL}s)")

    async def shutdown(self) -> None:
        """停止刷盘任务并做最终 flush"""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        # 最终刷盘
        await self.flush()
        log.info("[STATS] StatsCollector shutdown complete (final flush done)")


# 全局单例
stats_collector = StatsCollector()
