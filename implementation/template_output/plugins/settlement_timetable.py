from __future__ import annotations

from typing import Any

import pendulum
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from pendulum import DateTime


class BerlinSettlementTimetable(Timetable):
    periodic = True

    def __init__(
        self,
        timezone_name: str = "Europe/Berlin",
        schedule_hour: int = 6,
        schedule_minute: int = 30,
    ) -> None:
        self.timezone_name = timezone_name
        self.schedule_hour = schedule_hour
        self.schedule_minute = schedule_minute
        self._timezone = pendulum.timezone(timezone_name)

    @property
    def summary(self) -> str:
        return (
            f"local settlement day; run next day "
            f"{self.schedule_hour:02d}:{self.schedule_minute:02d} "
            f"{self.timezone_name}"
        )

    def serialize(self) -> dict[str, Any]:
        return {
            "timezone_name": self.timezone_name,
            "schedule_hour": self.schedule_hour,
            "schedule_minute": self.schedule_minute,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "BerlinSettlementTimetable":
        return cls(**data)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        local_trigger = run_after.in_timezone(self._timezone)
        end = local_trigger.start_of("day")
        start = end.subtract(days=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            start = last_automated_data_interval.end.in_timezone(self._timezone)
        else:
            if restriction.earliest is None:
                return None
            earliest = restriction.earliest.in_timezone(self._timezone)
            start = earliest.start_of("day")
            if earliest > start:
                start = start.add(days=1)
            if not restriction.catchup:
                today = pendulum.now(self._timezone).start_of("day")
                if start < today:
                    start = today
        if restriction.latest is not None:
            latest = restriction.latest.in_timezone(self._timezone)
            if start > latest:
                return None
        end = start.add(days=1)
        run_after = end.add(
            hours=self.schedule_hour,
            minutes=self.schedule_minute,
        )
        return DagRunInfo(
            run_after=run_after,
            data_interval=DataInterval(start=start, end=end),
        )


class SettlementTimetablePlugin(AirflowPlugin):
    name = "settlement_timetable_plugin"
    timetables = [BerlinSettlementTimetable]
