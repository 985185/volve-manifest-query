from __future__ import annotations

import os
import sys
import subprocess
from typing import Optional

import argparse

ENV_WELLS_DIR = "VOLVE_WELLS_DIR"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="volveq", description="Volve Manifest Query CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    serve_p = sub.add_parser("serve", help="Run the API server")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", default="8000")
    serve_p.add_argument("--wells-dir", default=os.getenv(ENV_WELLS_DIR, "wells"))
    serve_p.add_argument("--reload", action="store_true", help="Enable auto-reload")

    args = parser.parse_args(argv)

    if args.cmd == "serve":
        # Pass wells dir through env so api.py startup uses it
        env = os.environ.copy()
        env[ENV_WELLS_DIR] = args.wells_dir

        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "volve_query.api:app",
            "--host",
            args.host,
            "--port",
            str(args.port),
        ]
        if args.reload:
            cmd.append("--reload")

        return subprocess.call(cmd, env=env)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
