from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
WORK = ROOT / "work-reference"


if WORK.exists():
    shutil.rmtree(WORK)
WORK.mkdir()
with zipfile.ZipFile(ROOT / "task/输入数据包.zip") as package:
    package.extractall(WORK)
output = WORK / "output"
env = os.environ.copy()
env["AIRFLOW_HOME"] = str(WORK / ".airflow-home")
completed = subprocess.run([
    sys.executable, str(ROOT / "implementation/build_delivery.py"),
    "--input", str(WORK / "input_data"), "--output", str(output),
], cwd=ROOT, env=env, text=True, capture_output=True, timeout=600)
if completed.returncode != 0:
    raise SystemExit(completed.stdout + completed.stderr)
EVIDENCE.mkdir(exist_ok=True)
candidate = EVIDENCE / "reference-candidate.zip"
with zipfile.ZipFile(candidate, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in sorted(output.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            archive.write(path, path.relative_to(WORK).as_posix())
summary = {
    "result": "PASS", "mode": "reference", "commit_sha": os.getenv("GITHUB_SHA"),
    "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
    "reference_members": sorted(path.relative_to(WORK).as_posix() for path in output.rglob("*") if path.is_file() and "__pycache__" not in path.parts),
}
(EVIDENCE / "reference-generation.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
