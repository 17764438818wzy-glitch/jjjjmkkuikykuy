"""Compare batch candidates against the real single-chunk route's behavior."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from test_chunk_api_nonblocking import Vector, harness  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]


def candidate():
    path = ROOT / "candidate-server/paperflow_batch_vectors.py"
    assert path.is_file(), "batch vector compatibility code is not implemented"
    spec = importlib.util.spec_from_file_location("candidate_batch_vectors", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Model:
    def __init__(self):
        self.calls = []

    def encode(self, texts):
        self.calls.append(list(texts))
        vectors = [Vector([float(sum(text.encode("utf8")) % 997), float(len(text))]) for text in texts]
        return vectors, sum(len(text) for text in texts)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [8, 16])
async def test_batch_vectors_match_single_chunk_route(harness, batch_size):
    module = candidate()
    state, app, namespace = harness
    requests = [{"content": f"text-{i}", "user_id": "author",
                 **({"questions": [" question "]} if i == 2 else {})} for i in range(17)]
    direct = Model()
    namespace["TenantLLMService"].model_instance = lambda _: direct
    client = app.test_client()
    for req in requests:
        response = await client.post("/datasets/ds/documents/paper-1/chunks", json=req)
        assert (await response.get_json())["code"] == 0
    expected = {row["content_with_weight"]: row["q_2_vec"] for row in state.documents.values()}
    model = Model()
    result = module.encode_batches(model, "Paper paper-1", requests, max_chunks=batch_size,
        max_batch_tokens=10000, max_input_tokens=1000, count_tokens=len,
        normalize_user=namespace["normalize_llm_user_id"], encode_with_user=namespace["_encode_with_request_user"])
    assert [row["vector"] for row in result["items"]] == [expected[r["content"]] for r in requests]
    assert result["token_count"] == sum(sum(len(text) for text in call) for call in direct.calls)
    assert max(len(call) for call in model.calls) <= batch_size * 2
    assert len(model.calls) == (17 + batch_size - 1) // batch_size


def test_batch_token_budget_is_enforced_before_model_call():
    module = candidate()
    model = Model()
    requests = [{"content": "short", "user_id": "a"}, {"content": "x" * 50, "user_id": "a"}]
    with pytest.raises(ValueError):
        module.encode_batches(model, "title", requests, max_chunks=8, max_batch_tokens=20,
            max_input_tokens=100, count_tokens=len, normalize_user=lambda u: u,
            encode_with_user=lambda mdl, texts, _: mdl.encode(texts))
    assert model.calls == []


def test_batches_split_by_total_tokens_and_never_mix_users():
    module = candidate()
    model = Model()
    requests = [{"content": "abc", "user_id": "a"} for _ in range(3)]
    result = module.encode_batches(model, "title", requests, max_chunks=8, max_batch_tokens=16,
        max_input_tokens=100, count_tokens=len, normalize_user=lambda u: u,
        encode_with_user=lambda mdl, texts, _: mdl.encode(texts))
    assert len(result["items"]) == 3
    assert [sum(len(t) for t in call) for call in model.calls] == [16, 8]
    with pytest.raises(ValueError):
        module.encode_batches(model, "title", [{"content": "x", "user_id": "a"},
                                                {"content": "y", "user_id": "b"}],
            max_chunks=8, max_batch_tokens=100, max_input_tokens=100, count_tokens=len,
            normalize_user=lambda u: u, encode_with_user=lambda mdl, texts, _: mdl.encode(texts))
    assert len(model.calls) == 2
