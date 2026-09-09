from src.utils import GEMINICLI_USER_AGENT, get_geminicli_user_agent

EXPECTED_BASE_USER_AGENT = (
    "Mozilla/5.0 (compatible; Google-Gemini-CLI/1.0; "
    "+https://github.com/google-gemini/gemini-cli)"
)


def test_geminicli_user_agent_without_model():
    assert get_geminicli_user_agent() == EXPECTED_BASE_USER_AGENT
    assert GEMINICLI_USER_AGENT == EXPECTED_BASE_USER_AGENT


def test_geminicli_user_agent_includes_model_suffix():
    assert (
        get_geminicli_user_agent("gemini-3.1-flash-image")
        == f"{EXPECTED_BASE_USER_AGENT} gemini-3.1-flash-image"
    )
