from __future__ import annotations

import json
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from include.settlement_periods import load_inventory, parse_utc, summarize
from settlement_timetable import BerlinSettlementTimetable


@dag(
    dag_id="berlin_market_settlement",
    schedule=BerlinSettlementTimetable(
        timezone_name="Europe/Berlin",
        schedule_hour=6,
        schedule_minute=30,
    ),
    start_date=pendulum.datetime(2026, 3, 29, tz="Europe/Berlin"),
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["settlement", "dst"],
)
def berlin_market_settlement():
    @task(retries=0)
    def capture_interval() -> dict[str, str]:
        context = get_current_context()
        return {
            "start": context["data_interval_start"].isoformat(),
            "end": context["data_interval_end"].isoformat(),
        }

    @task(retries=1)
    def build_period_inventory(window: dict[str, str]) -> dict[str, object]:
        input_root = Path(os.environ["TASK_INPUT_ROOT"])
        inventory = load_inventory(
            input_root / "readings/meter_intervals.csv",
            "Europe/Berlin",
        )
        interval_start = parse_utc(window["start"])
        interval_end = parse_utc(window["end"])
        selected = [
            row
            for row in inventory
            if interval_start
            <= parse_utc(str(row["interval_start_utc"]))
            < interval_end
        ]
        return {"window": window, "daily": summarize(selected)}

    @task(retries=0)
    def validate_dst_contract(payload: dict[str, object]) -> dict[str, object]:
        daily = payload["daily"]
        if len(daily) != 1:
            raise ValueError("each run must contain one local settlement day")
        if daily[0]["slots_per_meter"] not in {92, 96, 100}:
            raise ValueError("unexpected DST-aware slot count")
        return payload

    @task(retries=0)
    def publish_settlement(payload: dict[str, object]) -> str:
        output_root = Path(os.environ["TASK_OUTPUT_ROOT"])
        output_root.mkdir(parents=True, exist_ok=True)
        target = output_root / "settlement_run.json"
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return str(target)

    publish_settlement(
        validate_dst_contract(
            build_period_inventory(capture_interval())
        )
    )


berlin_market_settlement()
