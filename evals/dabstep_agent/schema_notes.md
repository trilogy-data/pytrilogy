# Schema notes (read alongside manual.md)

The documentation in `manual.md` describes the fee rules (`fees.json`) with
list-valued matching fields. In this database those lists are unnested into
child tables:

| Doc field (fees.json)     | Table                          | Columns            |
|---------------------------|--------------------------------|--------------------|
| `account_type` (list)     | `fee_account_types`            | `fee_id`, `account_type` |
| `merchant_category_code` (list) | `fee_merchant_category_codes` | `fee_id`, `mcc` |
| `aci` (list)              | `fee_acis`                     | `fee_id`, `aci`    |
| `acquirer` (merchant list)| `merchant_acquirers`           | `merchant`, `acquirer` |

All remaining scalar fee fields live on `fees` (keyed by `ID`).

IMPORTANT — "applies to all" semantics: an empty list in the source means the
fee rule is unconstrained on that dimension. After unnesting, that means a fee
with NO rows in a child table matches EVERY value of that dimension. The same
applies to NULL scalar fields (`is_credit`, `capture_delay`,
`monthly_fraud_level`, `monthly_volume`, `intracountry`): NULL means the rule
applies to all values of that field.
