from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "template_output"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    input_root = Path(args.input).resolve()
    output = Path(args.output).resolve()
    if output.exists():
        shutil.rmtree(output)
    try:
        shutil.copytree(TEMPLATE, output)
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join([str(output), str(output / "plugins"), env.get("PYTHONPATH", "")])
        env["AIRFLOW_HOME"] = str(output.parent / ".airflow-home")
        completed = subprocess.run([
            sys.executable, str(output / "include/build_evidence.py"), str(input_root), str(output)
        ], cwd=output, env=env, text=True, capture_output=True, timeout=300)
        if completed.returncode != 0:
            raise RuntimeError(completed.stdout + completed.stderr)
    except Exception:
        if output.exists():
            shutil.rmtree(output)
        raise


if __name__ == "__main__":
    main()
