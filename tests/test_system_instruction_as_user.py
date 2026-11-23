"""systemInstruction 转首条 user 消息回归测试。

覆盖:
- 开启配置后 systemInstruction 移除，系统指令以 <system-instructions> 包裹
  作为首条 user 消息插入 contents 开头
- 已有首条 user 消息时合并（系统指令 parts 前置）
- contents 缺失时创建列表；无 systemInstruction 时不变
- 非 text part（inlineData 等）保留，不丢弃
- 关闭配置时保持 systemInstruction 规范化原行为
- 入参不被修改（copy-on-write）
- 抗截断注入在该模式下合并进首条 user 消息，不重新引入 systemInstruction
"""
from src.converter import antigravity_fix, anti_truncation
from src.converter.antigravity_fix import (
    _convert_system_instruction_to_user_message,
    _merge_system_instruction_text,
    normalize_antigravity_request,
)
from src.converter.anti_truncation import apply_anti_truncation

MODEL = "gemini-4-carbon-medium"
SYSTEM_PARTS = [{"text": "You are a helpful coding assistant."}]


async def _async_true():
    return True


async def _async_false():
    return False


async def test_converted_to_first_user_message(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": SYSTEM_PARTS},
        "contents": [
            {"role": "user", "parts": [{"text": "帮我重构这个模块"}]},
            {"role": "model", "parts": [{"text": "好的，我先看一下……"}]},
            {"role": "user", "parts": [{"text": "先从 session_cache 开始"}]},
        ],
    }
    result = await normalize_antigravity_request(request)

    assert "systemInstruction" not in result
    first = result["contents"][0]
    assert first["role"] == "user"
    text = first["parts"][0]["text"]
    assert text == (
        "<system-instructions>\nYou are a helpful coding assistant.\n</system-instructions>"
    )
    # 系统指令 part 前置，原首条 user 文本紧随其后（同条消息内合并）
    assert first["parts"][1]["text"] == "帮我重构这个模块"
    # 后续历史保持不变
    assert result["contents"][1]["parts"][0]["text"] == "好的，我先看一下……"
    assert result["contents"][2]["parts"][0]["text"] == "先从 session_cache 开始"


async def test_merges_with_existing_first_user(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": [{"text": "SYS"}, {"text": "RULES"}]},
        "contents": [
            {"role": "user", "parts": [{"text": "hello"}]},
            {"role": "model", "parts": [{"text": "hi"}]},
            {"role": "user", "parts": [{"text": "next"}]},
        ],
    }
    result = await normalize_antigravity_request(request)

    assert "systemInstruction" not in result
    # 首条 user 消息合并（系统指令 parts 前置），不新增条目
    assert len(result["contents"]) == 3
    first_parts = result["contents"][0]["parts"]
    assert first_parts[0]["text"].startswith("<system-instructions>")
    assert "SYS" in first_parts[0]["text"] and "RULES" in first_parts[0]["text"]
    assert first_parts[1]["text"] == "hello"
    assert result["contents"][1]["role"] == "model"


async def test_creates_contents_when_missing(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {"model": MODEL, "systemInstruction": {"parts": SYSTEM_PARTS}}
    result = await normalize_antigravity_request(request)

    assert "systemInstruction" not in result
    assert result["contents"][0]["role"] == "user"
    assert result["contents"][0]["parts"][0]["text"].startswith("<system-instructions>")


async def test_no_system_instruction_unchanged(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    result = await normalize_antigravity_request(request)

    assert len(result["contents"]) == 1
    assert result["contents"][0]["parts"][0]["text"] == "hi"


async def test_non_text_parts_preserved(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    inline = {"inlineData": {"mimeType": "image/png", "data": "aGk="}}
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": [{"text": "SYS"}, inline]},
        "contents": [{"role": "user", "parts": [{"text": "看图"}]}],
    }
    result = await normalize_antigravity_request(request)

    first_parts = result["contents"][0]["parts"]
    assert first_parts[0]["text"].startswith("<system-instructions>")
    assert first_parts[1] == inline, "非文本 part 按原顺序保留"


async def test_disabled_keeps_system_instruction(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_false
    )
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": SYSTEM_PARTS},
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    result = await normalize_antigravity_request(request)

    assert "systemInstruction" in result
    assert result["systemInstruction"]["role"] == "user"
    assert result["contents"][0]["parts"][0]["text"] == "hi"


async def test_input_not_mutated(monkeypatch):
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": SYSTEM_PARTS},
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    snapshot = {
        "model": MODEL,
        "systemInstruction": {"parts": [dict(p) for p in SYSTEM_PARTS]},
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    await normalize_antigravity_request(request)
    assert request == snapshot


async def test_snake_case_key_converted(monkeypatch):
    """snake_case system_instruction 键也参与转换。"""
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "system_instruction": {"parts": SYSTEM_PARTS},
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    result = await normalize_antigravity_request(request)

    assert "systemInstruction" not in result
    assert "system_instruction" not in result
    assert result["contents"][0]["parts"][0]["text"].startswith("<system-instructions>")


def test_merge_system_instruction_text():
    si = {"parts": [{"text": "A"}, {"text": ""}, {"text": "B"}, {"text": "  "}, {"inlineData": {}}]}
    assert _merge_system_instruction_text(si) == "A\nB"
    assert _merge_system_instruction_text({"parts": []}) == ""
    assert _merge_system_instruction_text(None) == ""


def test_convert_function_direct():
    """直接调用转换函数：缺 contents 时插入，且移除 systemInstruction。"""
    result = {
        "systemInstruction": {"parts": [{"text": "SYS"}]},
        "contents": [{"role": "model", "parts": [{"text": "hi"}]}],
    }
    _convert_system_instruction_to_user_message(result)
    assert "systemInstruction" not in result
    assert result["contents"][0]["role"] == "user"
    assert result["contents"][1]["role"] == "model"


async def test_anti_truncation_merges_into_first_user(monkeypatch):
    """开关开启时抗截断控制指令合并进首条 user 消息，不重新引入 systemInstruction。"""
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    monkeypatch.setattr(
        anti_truncation, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "systemInstruction": {"parts": SYSTEM_PARTS},
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    normalized = await normalize_antigravity_request(request)
    payload = await apply_anti_truncation(
        {"model": normalized.pop("model"), "request": normalized}
    )

    req = payload["request"]
    assert "systemInstruction" not in req
    parts = req["contents"][0]["parts"]
    assert parts[0]["text"].startswith("<system-instructions>")
    assert "emit_answer" in parts[1]["text"], "控制指令应紧随系统指令之后"
    assert parts[2]["text"] == "hi"


async def test_anti_truncation_no_system_prepend(monkeypatch):
    """开关开启且请求无系统指令时，控制指令前置插入首条 user 消息。"""
    monkeypatch.setattr(
        antigravity_fix, "get_system_instruction_as_user_message", _async_true
    )
    monkeypatch.setattr(
        anti_truncation, "get_system_instruction_as_user_message", _async_true
    )
    request = {
        "model": MODEL,
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
    }
    normalized = await normalize_antigravity_request(request)
    payload = await apply_anti_truncation(
        {"model": normalized.pop("model"), "request": normalized}
    )

    req = payload["request"]
    assert "systemInstruction" not in req
    assert "emit_answer" in req["contents"][0]["parts"][0]["text"]
    assert req["contents"][0]["parts"][1]["text"] == "hi"


async def test_anti_truncation_disabled_keeps_system_instruction(monkeypatch):
    """开关关闭时抗截断保持原行为：控制指令追加到 systemInstruction.parts。"""
    monkeypatch.setattr(
        anti_truncation, "get_system_instruction_as_user_message", _async_false
    )
    payload = {
        "model": MODEL,
        "request": {
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "systemInstruction": {"parts": SYSTEM_PARTS},
        },
    }
    result = await apply_anti_truncation(payload)

    si = result["request"]["systemInstruction"]
    assert si["parts"][0]["text"] == SYSTEM_PARTS[0]["text"]
    assert "emit_answer" in si["parts"][1]["text"]
