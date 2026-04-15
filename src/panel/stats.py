"""
统计数据 API 路由
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query

from log import log
from src.utils import verify_panel_token

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get("/summary")
async def get_stats_summary(
    mode: str = "geminicli",
    start_time: Optional[int] = Query(None, description="起始时间 epoch 秒"),
    end_time: Optional[int] = Query(None, description="结束时间 epoch 秒"),
    _=Depends(verify_panel_token),
):
    """获取统计汇总（全局 + 模型维度 + 凭证维度），支持时间过滤"""
    try:
        from src.storage_adapter import get_storage_adapter
        adapter = await get_storage_adapter()
        summary = await adapter._backend.get_stats_summary(
            mode=mode, start_time=start_time, end_time=end_time
        )
        # 同时获取请求级统计（跟随mode过滤）
        if hasattr(adapter._backend, 'get_request_stats_summary'):
            request_summary = await adapter._backend.get_request_stats_summary(
                mode=mode, start_time=start_time, end_time=end_time
            )
            summary["request"] = request_summary
        return summary
    except Exception as e:
        log.error(f"[STATS API] Error getting summary: {e}")
        return {"global": {"total": 0, "success": 0, "fail": 0}, "models": [], "credentials": []}


@router.get("/credential/{filename}")
async def get_credential_stats(
    filename: str,
    mode: str = "geminicli",
    start_time: Optional[int] = Query(None),
    end_time: Optional[int] = Query(None),
    _=Depends(verify_panel_token),
):
    """获取单凭证的模型级统计明细"""
    try:
        from src.storage_adapter import get_storage_adapter
        adapter = await get_storage_adapter()
        details = await adapter._backend.get_stats_by_credential(
            filename, mode=mode, start_time=start_time, end_time=end_time
        )
        return {"filename": filename, "models": details}
    except Exception as e:
        log.error(f"[STATS API] Error getting credential stats: {e}")
        return {"filename": filename, "models": []}


@router.post("/reset")
async def reset_stats(mode: str = None, _=Depends(verify_panel_token)):
    """清零统计数据"""
    try:
        # 先 flush 内存中的数据
        from src.stats_collector import stats_collector
        await stats_collector.flush()

        from src.storage_adapter import get_storage_adapter
        adapter = await get_storage_adapter()
        ok = await adapter._backend.reset_stats(mode=mode)
        return {"success": ok, "message": f"统计数据已清零 (mode={mode or 'all'})"}
    except Exception as e:
        log.error(f"[STATS API] Error resetting stats: {e}")
        return {"success": False, "message": str(e)}
