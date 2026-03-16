import sys
from datetime import datetime, timezone

import pyarrow as pa

if __name__ == "__main__":
    table = pa.table(
        {
            "data_updated_through": pa.array(
                [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            )
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
