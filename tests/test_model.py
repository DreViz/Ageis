import json

import httpx
import pytest

from runtime.model import ModelError, ModelResult, OpenAIModel, grounded_proposal


def test_provider_request_strict_tool_and_usage():
    def handler(request):
        body = json.loads(request.content)
        assert request.url.host == "api.openai.com"
        assert body["model"] == "example-pinned-model"
        assert body["store"] is False and body["parallel_tool_calls"] is False
        assert body["max_output_tokens"] == 256
        assert all(t["strict"] and not t["parameters"]["additionalProperties"] for t in body["tools"])
        return httpx.Response(200, json={"status": "completed", "output": [{"type": "function_call",
            "name": "propose_access", "arguments": json.dumps({"user_id": "maya", "workspace_id": "analytics"})}],
            "usage": {"input_tokens": 21, "output_tokens": 7}})
    model = OpenAIModel("test", transport=httpx.MockTransport(handler))
    result = model.call("example-pinned-model", [{"role": "user", "content": "Maya needs analytics"}])
    model.close()
    assert result == ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}, 21, 7)


@pytest.mark.parametrize("output", [
    [],
    [{"type": "function_call", "name": "grant_access", "arguments": "{}"}],
    [{"type": "function_call", "name": "propose_access", "arguments": "not-json"}],
    [{"type": "function_call", "name": "propose_access", "arguments": '{"user_id":"maya","workspace_id":"analytics","extra":1}'}],
    [{"type": "function_call", "name": "request_input", "arguments": '{"question":"?"}'},
     {"type": "function_call", "name": "request_input", "arguments": '{"question":"?"}'}],
])
def test_provider_invalid_output_fails_closed(output):
    model = OpenAIModel("test", transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"status": "completed", "output": output})))
    with pytest.raises(ModelError):
        model.call("model", [{"role": "user", "content": "Maya analytics"}])
    model.close()


def test_proposals_must_be_grounded_in_user_text():
    result = ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"})
    assert grounded_proposal(result, [{"role": "user", "content": "Maya needs analytics"}]) == result.arguments
    assert grounded_proposal(result, [{"role": "user", "content": "Maya needs access"}]) is None
    assert grounded_proposal(result, [{"role": "user", "content": "Maya needs analytics-extra"}]) is None
    assert grounded_proposal(result, [{"role": "assistant", "content": "maya analytics"}]) is None
