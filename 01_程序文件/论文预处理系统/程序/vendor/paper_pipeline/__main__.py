"""Module entrypoint for paper_pipeline."""

from __future__ import annotations

import argparse
import json

from paper_pipeline.e2e.real_flow_50 import RealFlow50Config, run_real_flow_50


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m paper_pipeline")
    parser.add_argument("--input", dest="input_dir")
    parser.add_argument("--source", dest="source_dir")
    parser.add_argument("--library", dest="library_dir")
    parser.add_argument("--output-cleaned", dest="output_cleaned_dir")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--mode", default="dry-run-sql", choices=["dry-run-sql", "postgresql"])
    parser.add_argument("--copy-sample", action="store_true")
    parser.add_argument("--generate-sample", action="store_true")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--allow-less-than-50", action="store_true")
    parser.add_argument("--no-confirm-upload", action="store_true")
    args = parser.parse_args(argv)

    result = run_real_flow_50(
        RealFlow50Config(
            input_dir=args.input_dir,
            source_dir=args.source_dir,
            library_dir=args.library_dir,
            output_cleaned_dir=args.output_cleaned_dir,
            run_id=args.run_id,
            mode=args.mode,
            copy_sample=args.copy_sample,
            generate_sample=args.generate_sample,
            max_file_count=args.max_files,
            allow_less_than_50=args.allow_less_than_50,
            confirm_upload=not args.no_confirm_upload,
        ),
    )
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "evaluation_report": str(result.evaluation_report_json_path),
            },
            ensure_ascii=False,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
