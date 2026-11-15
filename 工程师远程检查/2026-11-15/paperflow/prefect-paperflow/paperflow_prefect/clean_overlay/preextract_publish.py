"""Reuse native publication; complete its exact context projection boundary."""
from pathlib import Path
import importlib.util
import os
import sys

_source = (Path(os.environ["PAPERFLOW_SELECTED_CORE"]).resolve().parent / "desktop" / "preextract_publish.py").resolve()
_name = "_paperflow_native_preextract_publish"
_native = sys.modules.get(_name)
if _native is None:
    _spec = importlib.util.spec_from_file_location(_name, _source)
    _native = importlib.util.module_from_spec(_spec)
    sys.modules[_name] = _native
    try:
        _spec.loader.exec_module(_native)
    except BaseException:
        sys.modules.pop(_name, None)
        raise
elif Path(_native.__file__).resolve() != _source:
    raise RuntimeError("Cannot mix native publication releases")

_BaseBudget = getattr(_native, "_paperflow_original_budget", _native._Budget)
_native._paperflow_original_budget = _BaseBudget


class _Budget(_BaseBudget):
    def contextual(self, text, context, render, identity):
        # The original overlong-context branch remains the first choice.
        if not self.fits(render(0, context + "。")):
            return _BaseBudget.contextual(self, text, context, render, identity)
        try:
            return _BaseBudget.contextual(self, text, context, render, identity)
        except ValueError as exc:
            if str(exc) != "token budget cannot fit required context":
                raise
            # Count errors with coincidentally equal text cannot authorize this
            # local recovery; require the real native split admission failure.
            trace = exc.__traceback__
            previous_code = last_code = None
            while trace is not None:
                previous_code, last_code = last_code, trace.tb_frame.f_code
                trace = trace.tb_next
            if previous_code is not _BaseBudget.split.__code__ or last_code is not _native._require.__code__:
                raise
            reference = "关联上下文：" + _native._sha(identity.encode())[:16] + "\n"
            if not self.fits(render(0, reference + "。")):
                raise
            # The sentinel may fit while a real character or later slice id
            # does not. Reuse the existing indexed projection, retaining every
            # context and content character, the limits and the exact counter.
            projected = context + text
            prefix, parts, metadata, preserved = _BaseBudget.contextual(self, projected, reference, render, identity)
            _native._require(prefix == reference and not metadata and preserved == projected
                             and "".join(parts) == projected, "context projection reassembly changed")
            return prefix, parts, {"context_sha256": _native._sha(context.encode()),
                                   "context_in_projection": True}, preserved


# Native prepare_publication keeps its own global namespace. Bind the narrow
# adapter there and export the existing API, including its private helpers.
_native._Budget = _Budget
from preextract_tree_relations import install, install_publication
install(Path(os.environ['PAPERFLOW_SELECTED_CORE']))
install_publication(_native)
for _key, _value in vars(_native).items():
    if not _key.startswith("__") and not _key.startswith("_paperflow_"):
        globals()[_key] = _value
