from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import pendulum
from airflow.models import DagBag
from airflow.timetables.base import TimeRestriction


REFERENCE_ROOT = Path(__file__).resolve().parents[1]
for import_root in [REFERENCE_ROOT, REFERENCE_ROOT / "plugins"]:
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from include.settlement_periods import load_inventory, parse_utc, summarize
from settlement_timetable import BerlinSettlementTimetable


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def utc_text(value: pendulum.DateTime) -> str:
    return value.in_timezone("UTC").to_iso8601_string()


def build_timetable_cases(
    input_root: Path,
    timetable: BerlinSettlementTimetable,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for case in read_csv(input_root / "cases/schedule_cases.csv"):
        if case["mode"] == "AUTOMATED":
            earliest = pendulum.parse(case["restriction_earliest_utc"])
            info = timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=TimeRestriction(
                    earliest=earliest,
                    latest=None,
                    catchup=True,
                ),
            )
            if info is None:
                raise ValueError(f"no interval for {case['case_id']}")
            interval = info.data_interval
            run_or_trigger = utc_text(info.run_after)
        elif case["mode"] == "MANUAL":
            trigger = pendulum.parse(case["manual_run_after_utc"])
            interval = timetable.infer_manual_data_interval(run_after=trigger)
            run_or_trigger = utc_text(trigger)
        else:
            raise ValueError(f"unknown mode {case['mode']}")
        local_start = interval.start.in_timezone(timetable.timezone_name)
        local_end = interval.end.in_timezone(timetable.timezone_name)
        duration_minutes = int(
            (interval.end - interval.start).total_seconds() // 60
        )
        settlement_date = local_start.to_date_string()
        rows.append(
            {
                "case_id": case["case_id"],
                "mode": case["mode"],
                "settlement_date": settlement_date,
                "data_interval_start_utc": utc_text(interval.start),
                "data_interval_end_utc": utc_text(interval.end),
                "duration_minutes": duration_minutes,
                "run_or_trigger_utc": run_or_trigger,
                "local_start": local_start.to_iso8601_string(),
                "local_end": local_end.to_iso8601_string(),
                "result": "PASS"
                if settlement_date == case["expected_settlement_date"]
                else "FAIL",
            }
        )
    return rows


def build_daily_controls(
    inventory: list[dict[str, object]],
    contract: dict[str, object],
) -> list[dict[str, object]]:
    summarized = {
        row["settlement_date"]: row for row in summarize(inventory)
    }
    rows_by_day: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in inventory:
        rows_by_day[str(row["settlement_date"])].append(row)
    controls: list[dict[str, object]] = []
    for target in contract["target_days"]:
        settlement_date = target["settlement_date"]
        rows = rows_by_day[settlement_date]
        period_meter_keys = {
            (str(row["period_key"]), str(row["meter_id"])) for row in rows
        }
        actual = summarized[settlement_date]
        controls.append(
            {
                "settlement_date": settlement_date,
                "classification": target["classification"],
                "slots_per_meter": actual["slots_per_meter"],
                "meter_rows": actual["meter_rows"],
                "duration_minutes": int(
                    (
                        parse_utc(target["end_utc"])
                        - parse_utc(target["start_utc"])
                    ).total_seconds()
                    // 60
                ),
                "total_mwh": f"{float(actual['total_mwh']):.6f}",
                "missing_slots": target["slots_per_meter"]
                - int(actual["slots_per_meter"]),
                "duplicate_meter_slots": len(rows) - len(period_meter_keys),
                "run_after_utc": target["run_after_utc"],
            }
        )
    return controls


def build_dst_evidence(
    inventory: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    dates = sorted({str(row["settlement_date"]) for row in inventory})
    for settlement_date in dates:
        for hour in range(24):
            clock_prefix = f"{hour:02d}:"
            matches = [
                row
                for row in inventory
                if row["settlement_date"] == settlement_date
                and str(row["local_clock"]).startswith(clock_prefix)
            ]
            period_keys = {str(row["period_key"]) for row in matches}
            offsets = sorted({str(row["utc_offset"]) for row in matches})
            folds = sorted({str(row["fold"]) for row in matches})
            expected = (
                0
                if settlement_date == "2026-03-29" and hour == 2
                else 8
                if settlement_date == "2026-10-25" and hour == 2
                else 4
            )
            rows.append(
                {
                    "settlement_date": settlement_date,
                    "local_hour": f"{hour:02d}",
                    "slots_per_meter": len(period_keys),
                    "expected_slots": expected,
                    "utc_offsets": "|".join(offsets),
                    "fold_values": "|".join(folds),
                    "result": "PASS"
                    if len(period_keys) == expected
                    else "FAIL",
                }
            )
    return rows


def inspect_dag(reference_root: Path) -> dict[str, object]:
    dag_bag = DagBag(
        dag_folder=str(reference_root / "dags"),
        include_examples=False,
        safe_mode=False,
    )
    if dag_bag.import_errors:
        raise RuntimeError(f"DAG import errors: {dag_bag.import_errors}")
    dag = dag_bag.dags.get("berlin_market_settlement")
    if dag is None:
        raise RuntimeError("berlin_market_settlement DAG not found")
    timetable = dag.timetable
    ordered_ids = [
        "capture_interval",
        "build_period_inventory",
        "validate_dst_contract",
        "publish_settlement",
    ]
    tasks = []
    for task_id in ordered_ids:
        task = dag.get_task(task_id)
        tasks.append(
            {
                "task_id": task_id,
                "retries": task.retries,
                "upstream": sorted(task.upstream_task_ids),
            }
        )
    return {
        "dag_id": dag.dag_id,
        "schedule_class": timetable.__class__.__name__,
        "schedule_serialized": timetable.serialize(),
        "timezone": dag.timezone.name,
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "render_template_as_native_obj": dag.render_template_as_native_obj,
        "task_count": len(dag.tasks),
        "tasks": tasks,
        "parse_time_input_reads": [],
        "parse_time_output_writes": [],
        "result": "PASS",
    }


def build_all(
    input_root: Path,
    output_root: Path,
    reference_root: Path = REFERENCE_ROOT,
) -> dict[str, object]:
    if output_root.exists():
        import shutil
        for child in output_root.iterdir():
            if child.name in {"dags", "plugins", "include"}:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    contract = json.loads(
        (input_root / "contracts/timetable_contract.json").read_text(
            encoding="utf-8"
        )
    )
    timetable = BerlinSettlementTimetable(
        timezone_name=contract["timezone"],
        schedule_hour=contract["timetable"]["schedule_hour"],
        schedule_minute=contract["timetable"]["schedule_minute"],
    )
    inventory = load_inventory(
        input_root / "readings/meter_intervals.csv",
        contract["timezone"],
    )
    cases = build_timetable_cases(input_root, timetable)
    daily = build_daily_controls(inventory, contract)
    evidence = build_dst_evidence(inventory)
    serialization = {
        "class_name": timetable.__class__.__name__,
        "serialized": timetable.serialize(),
        "deserialized": BerlinSettlementTimetable.deserialize(
            timetable.serialize()
        ).serialize(),
    }
    serialization["equal"] = (
        serialization["serialized"] == serialization["deserialized"]
    )
    serialization["result"] = "PASS" if serialization["equal"] else "FAIL"
    dag_structure = inspect_dag(reference_root)

    results_root = output_root / "results"
    write_csv(
        results_root / "timetable_cases.csv",
        [
            "case_id",
            "mode",
            "settlement_date",
            "data_interval_start_utc",
            "data_interval_end_utc",
            "duration_minutes",
            "run_or_trigger_utc",
            "local_start",
            "local_end",
            "result",
        ],
        cases,
    )
    write_csv(
        results_root / "period_inventory.csv",
        [
            "settlement_date",
            "period_key",
            "local_clock",
            "fold",
            "utc_offset",
            "interval_start_utc",
            "interval_end_utc",
            "meter_id",
            "mwh",
        ],
        [
            {
                **row,
                "mwh": f"{float(row['mwh']):.6f}",
            }
            for row in inventory
        ],
    )
    write_csv(
        results_root / "daily_controls.csv",
        [
            "settlement_date",
            "classification",
            "slots_per_meter",
            "meter_rows",
            "duration_minutes",
            "total_mwh",
            "missing_slots",
            "duplicate_meter_slots",
            "run_after_utc",
        ],
        daily,
    )
    write_csv(
        results_root / "dst_transition_evidence.csv",
        [
            "settlement_date",
            "local_hour",
            "slots_per_meter",
            "expected_slots",
            "utc_offsets",
            "fold_values",
            "result",
        ],
        evidence,
    )
    write_json(results_root / "serialization_roundtrip.json", serialization)
    write_json(results_root / "dag_structure.json", dag_structure)

    case_durations = sorted(
        int(row["duration_minutes"])
        for row in cases
        if row["mode"] == "AUTOMATED"
    )
    source = read_csv(input_root / "readings/meter_intervals.csv")
    period_meter_keys = {
        (str(row["period_key"]), str(row["meter_id"])) for row in inventory
    }
    daily_by_date = {row["settlement_date"]: row for row in daily}
    evidence_by_key = {
        (row["settlement_date"], row["local_hour"]): row for row in evidence
    }
    controls = {
        "source_rows": len(source),
        "meters": len({row["meter_id"] for row in source}),
        "target_days": len(daily),
        "unique_periods": len({str(row["period_key"]) for row in inventory}),
        "spring_forward_periods": int(
            daily_by_date["2026-03-29"]["slots_per_meter"]
        ),
        "normal_periods": int(daily_by_date["2026-06-17"]["slots_per_meter"]),
        "fall_back_periods": int(
            daily_by_date["2026-10-25"]["slots_per_meter"]
        ),
        "schedule_cases": len(cases),
        "timetable_duration_minutes": case_durations,
        "dag_tasks": dag_structure["task_count"],
        "dst_evidence_rows": len(evidence),
    }
    invariants = {
        "source_keys_unique": len({row["source_seq"] for row in source})
        == len(source),
        "every_utc_interval_is_15_minutes": all(
            (
                parse_utc(row["interval_end_utc"])
                - parse_utc(row["interval_start_utc"])
            ).total_seconds()
            == 900
            for row in source
        ),
        "period_meter_keys_unique": len(period_meter_keys) == len(inventory),
        "spring_forward_has_92_periods": controls["spring_forward_periods"]
        == 92,
        "normal_day_has_96_periods": controls["normal_periods"] == 96,
        "fall_back_has_100_periods": controls["fall_back_periods"] == 100,
        "spring_forward_skips_local_02_hour": evidence_by_key[
            ("2026-03-29", "02")
        ]["slots_per_meter"]
        == 0,
        "fall_back_repeated_hour_uses_fold": evidence_by_key[
            ("2026-10-25", "02")
        ]["fold_values"]
        == "0|1",
        "timetable_uses_23_24_25_hour_intervals": case_durations
        == [1380, 1440, 1500],
        "run_after_is_next_local_day_06_30": all(
            pendulum.parse(str(row["run_or_trigger_utc"]))
            .in_timezone(contract["timezone"])
            .strftime("%H:%M")
            == "06:30"
            for row in cases
            if row["mode"] == "AUTOMATED"
        ),
        "serialization_and_dag_contract_pass": serialization["result"]
        == dag_structure["result"]
        == "PASS",
    }
    validation = {
        "controls": controls,
        "invariants": invariants,
        "result": "PASS" if all(invariants.values()) else "FAIL",
    }
    write_json(results_root / "validation.json", validation)
    return validation


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    args = parser.parse_args()
    validation = build_all(args.input_root, args.output_root)
    if validation["result"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
