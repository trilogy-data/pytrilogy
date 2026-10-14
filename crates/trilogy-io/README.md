# trilogy-io

Write a program that is a [Trilogy](https://github.com/trilogy-data/pytrilogy)
data source. Return Arrow; get a command line, a filter/limit contract, and four
output formats for free.

```rust
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use trilogy_io::{Result, SourceRequest};

fn fib(request: &SourceRequest) -> Result<Vec<RecordBatch>> {
    let n = request.limit.unwrap_or(20);
    let mut values: Vec<i64> = vec![0, 1];
    while values.len() < n {
        let next = values[values.len() - 1] + values[values.len() - 2];
        values.push(next);
    }
    values.truncate(n);
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])?])
}

fn main() -> std::process::ExitCode {
    trilogy_io::source(fib)
        .pushdown(&[trilogy_io::Field::Limit])
        .run()
}
```

The binary implements Trilogy's script-datasource contract, so
`trilogy source describe|preview|check` work against it and any consumer that
invokes it gets the same answers as from a python source.

Point a datasource at it like a script. A path with no extension (or a `.exe`)
is run directly:

```
datasource fib (value: value) grain (value) file `./target/release/fib`;
```

## Resident mode (`vgi` feature)

Built with `--features vgi`, the same binary started with `--vgi` serves itself
as a pooled [VGI](https://query.farm/vgi/docs/rust/) worker. DuckDB (haybarn)
keeps it running and calls it once per scan, so a query no longer pays for a
process start. pytrilogy uses this automatically under
`python_datasource_transport = "vgi"`. A build without the feature refuses
`--vgi`, and pytrilogy then runs it once per scan instead.

The function must be `Fn + Send + Sync + 'static`, because a resident worker
calls it repeatedly. The `vgi` crate is Query Farm Source-Available licensed,
not OSI open source, so it is never a default feature. It needs Rust 1.97+.

## What you get

```
--limit N            --columns a,b,c        --filter 'col op value'  (repeatable)
--order-by k:desc,k2 --since VALUE          --partition k=v          (repeatable)
--format arrow|parquet|csv|json            --output URI
--describe
```

Output is an Arrow IPC stream on stdout by default.

## Pushdown is an optimization, never a requirement

`.pushdown(&[...])` declares the contract fields your source honors itself.
Everything you do not claim is enforced on your output stream instead, so
`--limit 10` and `--filter 'state = "CA"'` work correctly whether or not you did
anything about them. Claim a field when you can do better than a scan-and-discard
— an API-backed source claims `Filters` so it can push the predicate to the API.

One rule is applied for you: if there are filters *or an ordering* left for the
wrapper to run, `limit` is taken back off your hands -- a source that truncates
first and gets filtered or sorted second returns the wrong rows. Claim
`Field::OrderBy` alongside `Field::Limit` if you want to do top-N yourself.

## Return types

`IntoBatchReader` is implemented for `RecordBatch`, `Vec<RecordBatch>`,
`Option<RecordBatch>`, `Box<dyn RecordBatchReader + Send>`, `FFI_ArrowArrayStream`
(so polars and datafusion output works directly), and `Result<T, E>` for any
`T` above — so a fallible source needs no unwrapping.

## The contract, not the library

The flag names, exit codes, `--describe` payload and stream metadata keys are
identical to the ones `trilogy.io` implements in python, and are verified
against each other by a conformance suite in the pytrilogy repo. Trilogy does
not care which language a source is written in.

Failures exit **65** with a machine-readable line on stderr:

```
trilogy-io-error: {"type":"ContractError","message":"...","contract":1,"retryable":false}
```

## Versioning

Released on the same version stream as `pytrilogy` and `trilogy-parser`; a crate
version always corresponds to a real pytrilogy release.

## License

MIT
