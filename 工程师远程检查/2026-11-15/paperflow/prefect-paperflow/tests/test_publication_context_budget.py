"""Exercise the real publisher budget boundary with exact deterministic counts."""
import hashlib
import importlib.util
import os
from pathlib import Path
import sys

import pytest

CORE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORE))
from paperflow_prefect import legacy
from paperflow_prefect.settings import Settings
legacy.bind(Settings.load(CORE / "config.json"))
import preextract_publish as baseline

publisher = baseline
if os.environ.get("CONTEXT_PUBLISHER_CANDIDATE"):
    spec = importlib.util.spec_from_file_location("context_publisher_candidate", os.environ["CONTEXT_PUBLISHER_CANDIDATE"])
    publisher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publisher)

BaseBudget = getattr(publisher, "_BaseBudget", baseline._Budget)


class Counter:
    model_budget = 2000
    cl_limit = 2038

    def count_many(self, texts):
        return [{"model_tokens": len(text) // 2, "cl100k_tokens": len(text) + text.count("X")} for text in texts]


def render(index, text):
    return f"r{index}:" + text


def assert_projection(text, context):
    counter = Counter()
    budget = publisher._Budget(counter)
    prefix, parts, metadata, projected = budget.contextual(text, context, render, "chapter/测试")
    assert projected == context + text
    assert "".join(parts) == projected
    assert metadata == {"context_sha256": hashlib.sha256(context.encode()).hexdigest(), "context_in_projection": True}
    assert prefix == "关联上下文：" + hashlib.sha256("chapter/测试".encode()).hexdigest()[:16] + "\n"
    assert all(budget.fits(render(index, prefix + part)) for index, part in enumerate(parts))
    assert budget.model_budget == counter.model_budget == 2000
    assert budget.cl_limit == counter.cl_limit == 2038


def test_real_first_character_can_cost_more_than_dot_sentinel():
    context = "C" * 2034
    budget = publisher._Budget(Counter())
    assert budget.fits(render(0, context + "。"))
    assert not budget.fits(render(0, context + "X"))
    assert_projection("X世界\n原使用限制必须逐字保留", context)


def test_later_slice_number_can_exhaust_context_budget():
    context = "C" * 2034
    budget = publisher._Budget(Counter())
    assert budget.fits(render(0, context + "。"))
    assert budget.fits(render(9, context + "a"))
    assert not budget.fits(render(10, context + "a"))
    assert_projection("a" * 40, context)


def test_existing_overlong_context_projection_is_unchanged():
    args = ("完整正文" * 12, "C" * 3000, render, "chapter")
    assert publisher._Budget(Counter()).contextual(*args) == BaseBudget(Counter()).contextual(*args)


def test_ordinary_context_and_projection_are_unchanged():
    args = ("原正文\n含标点、限制和中文。", "论文：原题\n", render, "chapter")
    actual = publisher._Budget(Counter()).contextual(*args)
    assert actual == BaseBudget(Counter()).contextual(*args)
    assert actual[0] == args[1] and actual[2] == {} and actual[3] == args[0]


def test_empty_projection_is_still_rejected():
    with pytest.raises(ValueError, match="empty text cannot be split"):
        publisher._Budget(Counter()).contextual("", "context", render, "chapter")


def test_fixed_render_that_cannot_fit_is_still_rejected():
    fixed = lambda index, text: "F" * 2500 + text
    with pytest.raises(ValueError, match="token budget cannot fit required context"):
        publisher._Budget(Counter()).contextual("正文", "上下文", fixed, "chapter")


def test_short_reference_that_cannot_fit_is_still_rejected():
    class ReferenceLimit(Counter):
        def count_many(self, texts):
            counts = super().count_many(texts)
            for text, count in zip(texts, counts):
                if "关联上下文：" in text:
                    count["cl100k_tokens"] += 3000
            return counts
    with pytest.raises(ValueError, match="token budget cannot fit required context"):
        publisher._Budget(ReferenceLimit()).contextual("X正文", "C" * 2034, render, "chapter")


def test_other_counter_error_is_propagated():
    class Broken(Counter):
        def count_many(self, texts):
            raise ValueError("offline tokenizer artifact drift")
    with pytest.raises(ValueError, match="offline tokenizer artifact drift"):
        publisher._Budget(Broken()).contextual("正文", "上下文", render, "chapter")


def test_counter_deadline_is_not_a_context_recovery_signal():
    class TimedOut(Counter):
        def count_many(self, texts):
            raise TimeoutError("offline counter deadline exceeded")
    with pytest.raises(TimeoutError, match="offline counter deadline exceeded"):
        publisher._Budget(TimedOut()).contextual("正文", "上下文", render, "chapter")


def test_same_message_from_counter_is_not_native_split_admission():
    class CounterFailure(Counter):
        def __init__(self):
            self.calls = 0
        def count_many(self, texts):
            self.calls += 1
            if self.calls > 1:
                raise ValueError("token budget cannot fit required context")
            return super().count_many(texts)
    counter = CounterFailure()
    with pytest.raises(ValueError, match="token budget cannot fit required context"):
        publisher._Budget(counter).contextual("正文", "上下文", render, "chapter")
    assert counter.calls == 2


def test_native_require_called_inside_counter_is_not_split_admission():
    class CounterFailure(Counter):
        def __init__(self):
            self.calls = 0
        def count_many(self, texts):
            self.calls += 1
            if self.calls == 2:
                publisher._require(False, "token budget cannot fit required context")
            return super().count_many(texts)
    counter = CounterFailure()
    with pytest.raises(ValueError, match="token budget cannot fit required context"):
        publisher._Budget(counter).contextual("正文", "上下文", render, "chapter")
    assert counter.calls == 2


def test_actual_publication_uses_the_exported_budget():
    assert publisher.prepare_publication.__globals__["_Budget"] is publisher._Budget

