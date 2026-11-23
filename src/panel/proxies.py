"""Authenticated IP management; proxy passwords are never returned by reads."""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.credential_proxy import bind_proxy, check_proxy, delete_proxy, list_proxies, save_proxy
from src.storage.database import get_database
from src.utils import verify_panel_token

router = APIRouter(tags=["proxies"], dependencies=[Depends(verify_panel_token)])


class ProxyInput(BaseModel):
    name: str = Field(min_length=1, max_length=100, strict=True)
    # Omitting the URL on edit keeps the saved address and password.
    url: str | None = Field(default=None, max_length=2048, strict=True)


class ProxyBinding(BaseModel):
    proxy_id: str | None = Field(min_length=1, max_length=32, strict=True)


async def run(operation):
    try:
        return await operation
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/proxies")
async def proxy_list():
    return JSONResponse({"items": await list_proxies(await get_database())}, headers={"Cache-Control": "no-store"})


@router.post("/proxies")
async def proxy_create(body: ProxyInput):
    return await run(save_proxy(await get_database(), body.name, body.url))


@router.post("/proxies/{proxy_id}/test")
async def proxy_test(proxy_id: str):
    return await run(check_proxy(await get_database(), proxy_id))


@router.patch("/proxies/{proxy_id}")
async def proxy_update(proxy_id: str, body: ProxyInput):
    return await run(save_proxy(await get_database(), body.name, body.url, proxy_id))


@router.delete("/proxies/{proxy_id}")
async def proxy_delete(proxy_id: str):
    await run(delete_proxy(await get_database(), proxy_id))
    return {"success": True}


@router.patch("/creds/proxy/{email}")
async def credential_proxy(email: str, body: ProxyBinding):
    return await run(bind_proxy(await get_database(), email, body.proxy_id))
