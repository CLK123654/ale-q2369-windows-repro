from __future__ import annotations

import csv
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("UTC timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def period_key(instant_utc: datetime, timezone_name: str) -> tuple[str, str, int, str]:
    local = instant_utc.astimezone(ZoneInfo(timezone_name))
    settlement_date = local.date().isoformat()
    clock = local.strftime("%H:%M")
    key = f"{settlement_date}T{clock}#{local.fold}"
    offset = local.strftime("%z")
    offset = f"{offset[:3]}:{offset[3:]}"
    return settlement_date, key, local.fold, offset


def load_inventory(
    readings_path: Path,
    timezone_name: str = "Europe/Berlin",
) -> list[dict[str, object]]:
    with readings_path.open(encoding="utf-8", newline="") as handle:
        source = list(csv.DictReader(handle))
    required = {
        "source_seq",
        "interval_start_utc",
        "interval_end_utc",
        "meter_id",
        "mwh",
    }
    if not source or not required.issubset(source[0]):
        raise ValueError("readings columns are incomplete")
    source_sequences = [row["source_seq"] for row in source]
    if len(source_sequences) != len(set(source_sequences)):
        raise ValueError("duplicate source_seq")
    inventory: list[dict[str, object]] = []
    for row in source:
        start = parse_utc(row["interval_start_utc"])
        end = parse_utc(row["interval_end_utc"])
        if (end - start).total_seconds() != 900:
            raise ValueError(f"non-15-minute interval: {row['source_seq']}")
        mwh = float(row["mwh"])
        if not math.isfinite(mwh):
            raise ValueError(f"non-finite MWh: {row['source_seq']}")
        if not row["meter_id"]:
            raise ValueError(f"blank meter: {row['source_seq']}")
        settlement_date, key, fold, offset = period_key(start, timezone_name)
        inventory.append(
            {
                "settlement_date": settlement_date,
                "period_key": key,
                "local_clock": key[11:16],
                "fold": fold,
                "utc_offset": offset,
                "interval_start_utc": start.isoformat().replace("+00:00", "Z"),
                "interval_end_utc": end.isoformat().replace("+00:00", "Z"),
                "meter_id": row["meter_id"],
                "mwh": mwh,
            }
        )
    inventory.sort(
        key=lambda row: (
            row["settlement_date"],
            row["interval_start_utc"],
            row["meter_id"],
        )
    )
    keys = [(row["period_key"], row["meter_id"]) for row in inventory]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate period_key/meter_id")
    return inventory


def summarize(inventory: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in inventory:
        grouped[str(row["settlement_date"])].append(row)
    result = []
    for settlement_date, rows in sorted(grouped.items()):
        period_keys = {str(row["period_key"]) for row in rows}
        meters = {str(row["meter_id"]) for row in rows}
        result.append(
            {
                "settlement_date": settlement_date,
                "slots_per_meter": len(period_keys),
                "meter_rows": len(rows),
                "meters": len(meters),
                "total_mwh": round(sum(float(row["mwh"]) for row in rows), 6),
            }
        )
    return result
