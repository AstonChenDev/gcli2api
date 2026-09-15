import io
import json
import zipfile

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.panel import creds


class FakeStorageAdapter:
    def __init__(self, credentials):
        self.credentials = credentials
        self.requested = []

    async def list_credentials(self, mode="geminicli"):
        return list(self.credentials.get(mode, {}))

    async def get_credential(self, filename, mode="geminicli"):
        self.requested.append((filename, mode))
        return self.credentials.get(mode, {}).get(filename)


def read_zip(response):
    with zipfile.ZipFile(io.BytesIO(response.body)) as archive:
        return {
            filename: json.loads(archive.read(filename).decode("utf-8"))
            for filename in archive.namelist()
        }


@pytest.mark.asyncio
async def test_download_selected_creds_contains_only_selected_files(monkeypatch):
    adapter = FakeStorageAdapter(
        {
            "geminicli": {
                "alpha.json": {"token": "alpha"},
                "beta.json": {"token": "beta"},
                "not-selected.json": {"token": "other"},
            }
        }
    )

    async def fake_get_storage_adapter():
        return adapter

    monkeypatch.setattr(creds, "get_storage_adapter", fake_get_storage_adapter)

    response = await creds.download_selected_creds_common(["beta.json", "alpha.json", "beta.json"])

    assert response.media_type == "application/zip"
    assert response.headers["content-disposition"] == (
        "attachment; filename=selected_credentials.zip"
    )
    assert read_zip(response) == {
        "beta.json": {"token": "beta"},
        "alpha.json": {"token": "alpha"},
    }
    assert adapter.requested == [
        ("beta.json", "geminicli"),
        ("alpha.json", "geminicli"),
    ]


@pytest.mark.asyncio
async def test_download_selected_creds_uses_antigravity_storage(monkeypatch):
    adapter = FakeStorageAdapter({"antigravity": {"ag.json": {"refresh_token": "secret"}}})

    async def fake_get_storage_adapter():
        return adapter

    monkeypatch.setattr(creds, "get_storage_adapter", fake_get_storage_adapter)

    response = await creds.download_selected_creds_common(["ag.json"], mode="antigravity")

    assert response.headers["content-disposition"] == (
        "attachment; filename=antigravity_selected_credentials.zip"
    )
    assert read_zip(response) == {"ag.json": {"refresh_token": "secret"}}
    assert adapter.requested == [("ag.json", "antigravity")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename",
    ["../credential.json", "folder/credential.json", "folder\\credential.json", "credential.txt"],
)
async def test_download_selected_creds_rejects_invalid_filenames(monkeypatch, filename):
    async def fail_if_called():
        raise AssertionError("非法文件名不应访问存储")

    monkeypatch.setattr(creds, "get_storage_adapter", fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        await creds.download_selected_creds_common([filename])

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_download_selected_creds_reports_missing_file(monkeypatch):
    adapter = FakeStorageAdapter({"geminicli": {"exists.json": {"token": "ok"}}})

    async def fake_get_storage_adapter():
        return adapter

    monkeypatch.setattr(creds, "get_storage_adapter", fake_get_storage_adapter)

    with pytest.raises(HTTPException) as exc_info:
        await creds.download_selected_creds_common(["exists.json", "missing.json"])

    assert exc_info.value.status_code == 404
    assert "missing.json" in exc_info.value.detail
    assert adapter.requested == []


def test_download_selected_route_returns_zip(monkeypatch):
    adapter = FakeStorageAdapter({"geminicli": {"route.json": {"token": "route-test"}}})

    async def fake_get_storage_adapter():
        return adapter

    monkeypatch.setattr(creds, "get_storage_adapter", fake_get_storage_adapter)
    app = FastAPI()
    app.include_router(creds.router)
    app.dependency_overrides[creds.verify_panel_token] = lambda: "test-token"

    response = TestClient(app).post(
        "/creds/download-selected",
        json={"filenames": ["route.json"]},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == ["route.json"]


def test_desktop_and_mobile_pages_expose_selected_download_controls():
    from pathlib import Path

    expected_controls = [
        'id="batchDownloadBtn"',
        'id="antigravityBatchDownloadBtn"',
        "downloadSelectedCreds()",
        "downloadSelectedAntigravityCreds()",
    ]
    for page in ["front/control_panel.html", "front/control_panel_mobile.html"]:
        html = Path(page).read_text(encoding="utf-8")
        for control in expected_controls:
            assert html.count(control) == 1
