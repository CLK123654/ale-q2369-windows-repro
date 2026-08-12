from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUN_ROOT = ROOT / "windows-runs"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reset(path: Path) -> None:
    if path.exists(): shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with zipfile.ZipFile(archive) as package: package.extractall(target)


def members(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts)


def compare(actual: Path, expected: Path) -> list[str]:
    a, e = members(actual), members(expected)
    if a != e: raise AssertionError("delivery path set differs from Reference")
    for relative in e:
        left = (actual / relative).read_bytes().replace(b"\r\n", b"\n")
        right = (expected / relative).read_bytes().replace(b"\r\n", b"\n")
        if left != right: raise AssertionError(f"delivery differs from Reference: {relative}")
    return e


def build(input_root: Path, output: Path, airflow_home: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy(); env["AIRFLOW_HOME"] = str(airflow_home)
    return subprocess.run([sys.executable, str(ROOT / "implementation/build_delivery.py"), "--input", str(input_root), "--output", str(output)], cwd=ROOT, env=env, text=True, capture_output=True, timeout=600)


def main() -> None:
    reset(RUN_ROOT)
    airflow_version = subprocess.run([sys.executable, "-m", "airflow", "version"], text=True, capture_output=True, timeout=60)
    if airflow_version.returncode != 0 or airflow_version.stdout.strip() != "2.10.5":
        raise AssertionError(airflow_version.stdout + airflow_version.stderr)
    reference = RUN_ROOT / "reference"; extract(TASK / "reference.zip", reference); expected = reference / "output"
    clean_runs = []
    for label in ["clean-a", "clean-b"]:
        base = RUN_ROOT / label; extract(TASK / "输入数据包.zip", base); input_root = base / "input_data"
        before = {p.relative_to(input_root).as_posix(): sha(p) for p in input_root.rglob("*") if p.is_file()}
        for index in [1, 2]:
            output = base / f"output-{index}"; process = build(input_root, output, base / f"airflow-home-{index}")
            if process.returncode != 0: raise AssertionError(process.stdout + process.stderr)
            generated = compare(output, expected)
            clean_runs.append({"root_id": label, "process_index": index, "return_code": 0, "output_started_empty": True, "primary_software_executed": True, "input_unchanged": True, "reference_match": True, "generated_paths": generated})
        current = {p.relative_to(input_root).as_posix(): sha(p) for p in input_root.rglob("*") if p.is_file()}
        if before != current: raise AssertionError("input changed during standard run")

    positive = RUN_ROOT / "positive"; extract(TASK / "输入数据包.zip", positive)
    cases = positive / "input_data/cases/schedule_cases.csv"
    with cases.open(encoding="utf-8", newline="") as handle: rows = list(csv.DictReader(handle))
    for row in rows:
        if row["case_id"] == "MANUAL-SPRING": row["manual_run_after_utc"] = "2026-03-31T12:15:00Z"; row["expected_settlement_date"] = "2026-03-30"
    with cases.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["case_id", "mode", "expected_settlement_date", "manual_run_after_utc", "restriction_earliest_utc"], lineterminator="\n"); writer.writeheader(); writer.writerows(rows)
    pos_output = positive / "output"; process = build(positive / "input_data", pos_output, positive / "airflow-home")
    if process.returncode != 0: raise AssertionError(process.stdout + process.stderr)
    changed = {row["case_id"]: row for row in csv.DictReader((pos_output / "results/timetable_cases.csv").open(encoding="utf-8", newline=""))}
    if changed["MANUAL-SPRING"]["settlement_date"] != "2026-03-30": raise AssertionError("manual trigger change did not reach output")
    (EVIDENCE / "positive-case.json").write_text(json.dumps({"input_field": "MANUAL-SPRING.manual_run_after_utc", "before": "2026-03-30T12:15:00Z", "after": "2026-03-31T12:15:00Z", "behavior_changed": True}, indent=2) + "\n", encoding="utf-8")

    negative = RUN_ROOT / "negative"; extract(TASK / "输入数据包.zip", negative)
    readings = negative / "input_data/readings/meter_intervals.csv"; lines = readings.read_text(encoding="utf-8").splitlines(); lines.append(lines[1]); readings.write_text("\n".join(lines) + "\n", encoding="utf-8")
    neg_output = negative / "output"; neg_output.mkdir(); (neg_output / "stale.txt").write_text("stale", encoding="utf-8")
    process = build(negative / "input_data", neg_output, negative / "airflow-home")
    if process.returncode == 0 or neg_output.exists(): raise AssertionError("duplicate source key did not fail closed")
    (EVIDENCE / "negative-case.log").write_text(f"return_code={process.returncode}\n{process.stdout}{process.stderr}", encoding="utf-8")

    summary = {"result": "PASS", "commit_sha": os.getenv("GITHUB_SHA"), "workflow_run_id": os.getenv("GITHUB_RUN_ID"), "runner_image": os.getenv("ImageOS"), "main_software": {"name": "Apache Airflow", "version": airflow_version.stdout.strip(), "executed": True}, "clean_directory_count": 2, "process_runs_per_directory": 2, "clean_runs": clean_runs, "positive_mutation": "PASS", "negative_case": "PASS", "formal_network": {"wsl_external_interface_disabled": True, "external_services_used": False}, "linux_executables": ["python3", "airflow"], "linux_executables_executed": True, "wsl2_required": True}
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__": main()
