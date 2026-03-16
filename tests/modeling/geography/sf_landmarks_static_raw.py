import sys

import pyarrow as pa

if __name__ == "__main__":
    table = pa.table(
        {
            "OBJECTID": pa.array(["sf-1", "sf-2"], type=pa.string()),
            "city": pa.array(["USSFO", "USSFO"], type=pa.string()),
            "name": pa.array(["Landmark A", "Landmark B"], type=pa.string()),
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
