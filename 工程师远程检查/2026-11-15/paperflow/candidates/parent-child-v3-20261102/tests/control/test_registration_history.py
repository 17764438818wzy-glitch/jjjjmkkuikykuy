"""A resumed parent must keep old inventories without treating its output as history."""

import json
from pathlib import Path

import pytest

from paperflow_v3.control.classification import first_step_modules


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_registration_history_excludes_only_current_output_and_keeps_other_inventories(tmp_path):
    from paperflow_v3.control.classification import _registration_history_roots

    data = tmp_path / "data"
    v3 = data / "paperflow-v3"
    registration = v3 / "current-task" / "first-step" / "group-one" / "registration"
    registration.mkdir(parents=True)
    (registration / "existing-receipt.json").write_text("{}", encoding="utf8")
    places = [v3 / "old-task" / "paper", v3 / "current-task" / "first-step" / "group-two",
              v3 / "current-task" / "group-one" / "work", v3 / "current-task" / "first-step" / "group-one" / "classification",
              data / "legacy"]
    for index, place in enumerate(places):
        place.mkdir(parents=True, exist_ok=True)
        (place / "inventory.json").write_text(json.dumps({"synthetic": index}), encoding="utf8")
    original = (v3, data / "legacy")
    narrowed = _registration_history_roots(original, registration)
    found = {path.resolve() for root in narrowed for path in
             ([root] if root.is_file() else root.rglob("inventory.json"))}
    assert found == {(place / "inventory.json").resolve() for place in places}
    store = first_step_modules(FIRST_STEP)["receipts"].ReceiptStore(registration, history_roots=narrowed)
    store.check_output(tmp_path / "source")
    assert (registration / "existing-receipt.json").is_file()


def test_missing_or_inside_output_history_is_not_silently_discarded(tmp_path):
    from paperflow_v3.control.classification import _registration_history_roots

    missing = tmp_path / "missing"
    output = tmp_path / "output"
    inside = output / "unknown-history"
    assert _registration_history_roots((missing, inside), output) == (missing.resolve(), inside.resolve())


def test_inventory_in_current_output_requires_review(tmp_path):
    from paperflow_v3.control.classification import _registration_history_roots

    output = tmp_path / "data" / "registration"
    output.mkdir(parents=True)
    (output / "inventory.json").write_text("{}", encoding="utf8")
    with pytest.raises(ValueError, match="历史清单"):
        _registration_history_roots((output.parent,), output)
