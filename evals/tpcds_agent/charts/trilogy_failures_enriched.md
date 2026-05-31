# Trilogy failure analysis — 20260531-004042

- Run `20260531-004042` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 44 | failed: 4 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 3 | 75% |
| `undefined-concept` | 1 | 25% |

## Detail

### `syntax-parse`

- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

# Define per-bucket aggregates using inline filters on quantity ranges
…97, bucket4_discount, bucket4_netpaid) as bucket_61_80,
    case(bucket5_count > 165306, bucket5_discount, bucket5_netpaid) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 4, column 98.
  Expected one of:
          * _TERMINATOR

  Location:
  ...es.quantity between 1 and 20) ??? as bucket1_count; auto bucket_...

  Write stats: received 2363 chars / 2363 bytes; tail: …'bucket5_discount,
  bucket5_netpaid) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

# define counts and aggregates per bucket
auto bucket_1_20_lines <- cou…ucket_61_80_netpaid) as bucket_61_80,
    case(bucket_81_100_lines > 165306, bucket_81_100_discount, bucket_81_100_netpaid) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 23, column 80.
  Expected one of:
          * WHEN

  Location:
  ...iscount, bucket_1_20_netpaid) ??? as bucket_1_20,     case(bucke...

  Write stats: received 2129 chars / 2129 bytes; tail: …'0_discount,
  bucket_81_100_netpaid) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

# define counts and aggregates per bucket
auto bucket_1_20_lines <- cou…count, bucket_61_80_netpaid) as bucket_61_80,
    case(show_discount_81_100, bucket_81_100_discount, bucket_81_100_netpaid) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 30, column 73.
  Expected one of:
          * WHEN

  Location:
  ...iscount, bucket_1_20_netpaid) ??? as bucket_1_20,     case(show_...

  Write stats: received 2416 chars / 2416 bytes; tail: …'0_discount,
  bucket_81_100_netpaid) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['physical_sales.item.current_price']")
  ```
