# Trilogy failure analysis — 20260911-040324

- Run `20260911-040324_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 191 | failed: 16 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 10 | 62% |
| `join-resolution` | 4 | 25% |
| `syntax-parse` | 2 | 12% |

## Detail

### `undefined-concept`

- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  Syntax error in probe_f.preql: Undefined concept: pa.policy_identifier. Suggestions: ['pa.Policy.policy_identifier', 'pa.Policy_Coverage_Detail.Policy.policy_identifier', 'pa.policy_amount_identifier', 'pa.policy_amount', 'p.policy_identifier']
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Syntax error in probe_h.preql: Undefined concept: pa.policy_identifier. Suggestions: ['pa.Policy.policy_identifier', 'pa.Policy_Coverage_Detail.Policy.policy_identifier', 'pa.policy_amount_identifier', 'pa.policy_amount', 'p.policy_identifier']
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: pa.policy_identifier (line 7, col 3, in SELECT). Suggestions: ['pa.Policy.policy_identifier', 'pa.Policy_Coverage_Detail.Policy.policy_identifier', 'pa.policy_amount_identifier', 'pa.policy_amount']
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: 2 undefined concept references; fix all before re-running:
    - lp.claim_amount (line 2, col 8, in SELECT); did you mean: lp.claim_amount_identifier?
    - lp.Claim.company_claim_number (line 2, col 25, in SELECT)
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Syntax error in probe_a.preql: Undefined concept: pr.premium_paid_amount (line 2, col 37, in SELECT).
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: Undefined concept: pr.premium_amount (line 2, col 8, in SELECT).
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: Undefined concept: pr.policy_amount (line 2, col 8, in SELECT). Suggestions: ['pr.policy_amount_identifier']
  ```
- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  Syntax error in probe_f.preql: Undefined concept: pr.premium_paid (line 2, col 8, in SELECT).
  ```
- `trilogy file write probe_g.preql --run-and-delete`

  ```text
  Syntax error in probe_g.preql: Undefined concept: pr.premium_identifier (line 2, col 8, in SELECT). Suggestions: ['pr.policy_amount_identifier']
  ```
- `trilogy file write probe_fk.preql --run-and-delete`

  ```text
  Syntax error in probe_fk.preql: Undefined concept: cc.policy_coverage_detail_identifier. Suggestions: ['cc.Policy_Coverage_Detail.policy_coverage_detail_identifier', 'cc.Policy_Coverage_Detail.coverage_identifier', 'cc.Policy_Coverage_Detail.Policy.policy_identifier']
  ```

### `join-resolution`

- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {p.policy_number}; {pa.amount_type_code, pa.insurance_type_code, pa.policy_amount, pa.policy_amount_identifier}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {p.policy_number}; {pa.policy_amount, pa.policy_amount_identifier}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Resolution error in probe_d.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {p.policy_number}; {pcd.coverage_identifier, pcd.policy_coverage_detail_identifier}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {pa.amount_type_code, pa.policy_amount}; {pr.policy_amount_identifier}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...e apr.party_role_code = 'PH'  ??? subset join pa.policy_amount_i...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | by ca.Claim.company_claim_number;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ount_identifier) as loss_pay  ??? by ca.Claim.company_claim_numb...
  ```
