"""CLI entrypoint for the local paper pipeline workbench."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_pipeline.app.app_state import WorkbenchState
from paper_pipeline.app.controller import WorkbenchController


def start_workbench(controller: WorkbenchController) -> WorkbenchState:
    """Start the workbench shell by loading status only."""
    return controller.load_status()


def build_default_controller(base_dir: str | Path | None = None) -> WorkbenchController:
    root = Path(base_dir) if base_dir is not None else Path.cwd() / ".paper_pipeline_workbench"
    return WorkbenchController(
        input_dir=root / "input_raw",
        library_dir=root / "library",
        output_cleaned_dir=root / "output_cleaned",
        runs_dir=root / "library" / "runs",
        workbench_dir=root,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="paper-pipeline-workbench")
    parser.add_argument("--json", action="store_true", help="Print status as JSON.")
    parser.add_argument(
        "--base-dir",
        default="",
        help="Safe local workbench directory; defaults to .paper_pipeline_workbench.",
    )
    args = parser.parse_args(argv)
    controller = build_default_controller(args.base_dir or None)
    state = start_workbench(controller)
    if args.json:
        print(json.dumps(state.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(_render_state(state))
    return 0


def _render_state(state: WorkbenchState) -> str:
    return "\n".join(
        [
            "Paper Pipeline Workbench",
            f"Service URL: {state.service_url}",
            f"Current run: {state.current_run_id or 'not_started'}",
            f"Run status: {state.run_status}",
            "Flow entry: real_flow_50",
            f"Current stage: {state.current_stage}",
            (
                "Status counts: "
                f"completed={state.completed_count}, total={state.new_file_count}, "
                f"pending={state.pending_count}, failed={state.failed_count}"
            ),
            f"Next: {state.suggested_next_action}",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
