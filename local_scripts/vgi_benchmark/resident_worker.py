# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "vgi-python"]
# ///
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

BATCH = 65536
ids = pa.array(range(BATCH), pa.int64())
FULL = pa.RecordBatch.from_arrays(
    [
        ids,
        pa.array([i * 0.5 for i in range(BATCH)]),
        pa.array([f"label_{i % 1000}" for i in range(BATCH)]),
    ],
    names=["id", "value", "label"],
)


@dataclass(frozen=True, kw_only=True)
class SrcArgs:
    rows: Annotated[int, Arg(0)]


@dataclass(kw_only=True)
class SrcState(ArrowSerializableDataclass):
    left: int = 0


@init_single_worker
class Src(TableFunctionGenerator[SrcArgs, SrcState]):
    class Meta:
        name = "src"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[SrcArgs]) -> BindResponse:
        return BindResponse(output_schema=FULL.schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[SrcArgs]) -> SrcState:
        return SrcState(left=params.args.rows)

    @classmethod
    def process(cls, params, state: SrcState, out: OutputCollector) -> None:
        if state.left <= 0:
            out.finish()
            return
        n = min(state.left, BATCH)
        batch = FULL.slice(0, n)
        out.emit(
            pa.RecordBatch.from_arrays(
                [batch.column(c) for c in params.output_schema.names],
                schema=params.output_schema,
            )
        )
        state.left -= n


class ResidentWorker(Worker):
    catalog = Catalog(
        name="resident",
        default_schema="main",
        schemas=[Schema(path=["main"], functions=[Src])],
    )


if __name__ == "__main__":
    ResidentWorker().run()
