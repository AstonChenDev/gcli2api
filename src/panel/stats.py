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


@router.get("/request-timeseries")
async def get_request_timeseries(
    mode: str = "geminicli",
    start_time: Optional[int] = Query(None, description="起始时间 epoch 秒"),
    end_time: Optional[int] = Query(None, description="结束时间 epoch 秒"),
    _=Depends(verify_panel_token),
):
    """获取请求级统计的时间序列数据，用于图表展示"""
    try:
        from src.storage_adapter import get_storage_adapter
        adapter = await get_storage_adapter()
        if hasattr(adapter._backend, 'get_request_stats_timeseries'):
            data = await adapter._backend.get_request_stats_timeseries(
                mode=mode, start_time=start_time, end_time=end_time
            )
            return {"series": data}
        return {"series": []}
    except Exception as e:
        log.error(f"[STATS API] Error getting request timeseries: {e}")
        return {"series": []}


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

# HTTP 错误码描述映射（仅作为回退）
_ERROR_CODE_LABELS = {
    0: "Internal Error",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    429: "Rate Limited",
    500: "Server Error",
    503: "Service Unavailable",
}


def _extract_short_description(raw_desc: str, max_len: int = 120) -> str:
    """从上游原始错误响应中提取简短描述"""
    if not raw_desc:
        return ""
    # 尝试解析 JSON 格式的错误体
    try:
        import json
        data = json.loads(raw_desc)
        # Google API 通常返回 {"error": {"message": "...", "status": "..."}}
        err = data.get("error", {})
        if isinstance(err, dict):
            msg = err.get("message", "") or err.get("status", "")
            if msg:
                return msg[:max_len]
        # 有些返回 {"error": "string"}
        if isinstance(err, str) and err:
            return err[:max_len]
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    # 非 JSON，直接截断
    return raw_desc[:max_len]


@router.get("/error-codes")
async def get_error_code_stats(
    mode: str = "geminicli",
    start_time: Optional[int] = Query(None, description="起始时间 epoch 秒"),
    end_time: Optional[int] = Query(None, description="结束时间 epoch 秒"),
    _=Depends(verify_panel_token),
):
    """获取错误码分布统计"""
    try:
        from src.storage_adapter import get_storage_adapter
        adapter = await get_storage_adapter()
        if hasattr(adapter._backend, 'get_error_code_stats'):
            data = await adapter._backend.get_error_code_stats(
                mode=mode, start_time=start_time, end_time=end_time
            )
            # 补充 label：优先使用上游描述，回退到硬编码
            for item in data.get("summary", []):
                code = item["error_code"]
                upstream_desc = _extract_short_description(item.get("description", ""))
                item["label"] = upstream_desc or _ERROR_CODE_LABELS.get(code, f"HTTP {code}")
            return data
        return {"summary": [], "by_model": []}
    except Exception as e:
        log.error(f"[STATS API] Error getting error code stats: {e}")
        return {"summary": [], "by_model": []}


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

