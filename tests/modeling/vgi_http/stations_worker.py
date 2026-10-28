"""Fixed station and departure tables served over HTTP for the vgi_http tests.

Run it: `vgi-serve tests/modeling/vgi_http/stations_worker.py --http`
"""

from dataclasses import dataclass
from typing import ClassVar

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

STATIONS = pa.table(
    {
        "station_code": ["TRILOGY_TESTER_A", "TRILOGY_TESTER_B", "TRILOGY_TESTER_C"],
        "name": ["Tester Alpha", "Tester Beta", "Tester Gamma"],
        "region": ["north", "north", None],
        "lat": [10.0, 10.5, 11.0],
        "lng": [20.0, 20.5, 21.0],
    }
)
DEPARTURES = pa.table(
    {
        "station_code": [
            "TRILOGY_TESTER_A",
            "TRILOGY_TESTER_A",
            "TRILOGY_TESTER_A",
            "TRILOGY_TESTER_B",
            "TRILOGY_TESTER_C",
            "TRILOGY_TESTER_C",
        ],
        "train_number": ["T1", "T2", "T3", "T4", "T5", "T6"],
        "delay_minutes": [0, 5, 10, 2, 7, 1],
    }
)


@dataclass(frozen=True)
class NoArgs:
    pass


@dataclass(kw_only=True)
class Done(ArrowSerializableDataclass):
    done: bool = False


class FixedTable(TableFunctionGenerator[NoArgs, Done]):
    DATA: ClassVar[pa.Table]

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> Done:
        return Done()

    @classmethod
    def process(
        cls, params: ProcessParams[NoArgs], state: Done, out: OutputCollector
    ) -> None:
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(cls.DATA.select(params.output_schema.names).to_batches()[0])


@init_single_worker
@bind_fixed_schema
class StationsScan(FixedTable):
    DATA = STATIONS
    FIXED_SCHEMA = STATIONS.schema

    class Meta:
        name = "stations_scan"
        projection_pushdown = True


@init_single_worker
@bind_fixed_schema
class DeparturesScan(FixedTable):
    DATA = DEPARTURES
    FIXED_SCHEMA = DEPARTURES.schema

    class Meta:
        name = "departures_scan"
        projection_pushdown = True


class StationsWorker(Worker):
    catalog = Catalog(
        name="stations",
        default_schema="main",
        schemas=[
            Schema(
                path=["main"],
                tables=[
                    Table(name="stations", function=StationsScan),
                    Table(name="departures", function=DeparturesScan),
                ],
            )
        ],
    )


if __name__ == "__main__":
    StationsWorker.main()
