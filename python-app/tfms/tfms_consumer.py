#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.tfms_parser import parse_tfms_xml


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse TFMS XML payloads into JSON.")
    parser.add_argument("--input", "-i", nargs="*", help="Input file(s)")
    parser.add_argument("--stdin", action="store_true", help="Read XML from stdin")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    parser.add_argument("--output", "-o", help="Write JSON to file")
    args = parser.parse_args()

    if bool(args.stdin) == bool(args.input):
        parser.error("Use either --stdin or --input")

    results = {}
    if args.stdin:
        results["stdin"] = parse_tfms_xml(sys.stdin.read())
    else:
        for item in args.input:
            path = Path(item)
            results[str(path)] = parse_tfms_xml(path.read_text(encoding="utf-8"))

    content = json.dumps(results, ensure_ascii=False, indent=2 if args.pretty else None)
    if args.output:
        Path(args.output).write_text(content + "\n", encoding="utf-8")
    else:
        print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
