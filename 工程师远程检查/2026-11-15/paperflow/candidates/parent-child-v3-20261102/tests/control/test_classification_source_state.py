"""A pause retry keeps the original source identity while groups write concurrently."""

from types import SimpleNamespace

import pytest

from paperflow_v3.control import classification


def test_pause_retry_does_not_forget_the_source_bound_before_other_groups_write(monkeypatch, tmp_path):
    class PauseRequested(Exception):
        pass

    class UnknownRequest(Exception):
        pass

    digest = ["first"]
    saved = []

    def process(_group, _library, _root, _judge, _reader, **kwargs):
        kwargs["source_binder"](digest[0], "group")
        raise PauseRequested()

    modules = {
        "control": SimpleNamespace(PauseRequested=PauseRequested, UnknownRequest=UnknownRequest),
        "classification": SimpleNamespace(_group_id=lambda *_: "group", process_one_group=process),
        "sort": SimpleNamespace(_read_for_sort=lambda: None, _title_from_body=lambda: None),
    }
    monkeypatch.setattr(classification, "first_step_modules", lambda _: modules)
    monkeypatch.setattr(classification, "save_receipt_sync", lambda **record: saved.append(record))
    monkeypatch.setattr(classification, "read_receipts_sync", lambda _: [])
    source_state = {"data": None}
    args = dict(first_step_root=tmp_path, task_id="task", group_path=tmp_path / "group",
                library_root=tmp_path, work_root=tmp_path / "work", source_state=source_state,
                admission=SimpleNamespace(running_sync=lambda: True), nimble_endpoint="http://example.invalid",
                nimble_lock_path=tmp_path / "lock", history_roots=())

    with pytest.raises(PauseRequested):
        classification.classify_and_register(**args)
    assert source_state["data"] == {"digest": "first", "group_id": "group"}
    assert [record["kind"] for record in saved] == ["classification_source"]
    digest[0] = "changed"
    with pytest.raises(UnknownRequest):
        classification.classify_and_register(**args)
    assert len(saved) == 1
