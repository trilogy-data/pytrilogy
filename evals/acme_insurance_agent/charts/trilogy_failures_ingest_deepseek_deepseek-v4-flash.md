# Trilogy failure analysis — 20260911-020955

- Run `20260911-020955_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 217 | failed: 14 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 6 | 43% |
| `join-resolution` | 5 | 36% |
| `syntax-parse` | 3 | 21% |

## Detail

### `undefined-concept`

- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Syntax error in probe7.preql: Undefined concept: cc.policy_coverage_detail_identifier (line 5, col 3, in SELECT). Suggestions: ['cc.Policy_Coverage_Detail.policy_coverage_detail_identifier', 'cc.Policy_Coverage_Detail.coverage_identifier', 'cc.Policy_Coverage_Detail.Policy.policy_identifier']
  ```
- `trilogy file write probe10.preql --run-and-delete`

  ```text
  Syntax error in probe10.preql: Undefined concept: cc.zzz (line 4, col 3, in SELECT).
  ```
- `trilogy file write probe_raw.preql --run-and-delete`

  ```text
  Syntax error in probe_raw.preql: Undefined concept: Claim_Identifier. Suggestions: ['claim_identifier']
  ```
- `trilogy file write probe_lp.preql --run-and-delete`

  ```text
  Syntax error in probe_lp.preql: Undefined concept: lp.loss_payment_amount (line 2, col 8, in SELECT).
  ```
- `trilogy file write probe_lr.preql --run-and-delete`

  ```text
  Syntax error in probe_lr.preql: Undefined concept: lr.loss_reserve_amount (line 2, col 8, in SELECT).
  ```
- `trilogy file write answer_2306670096.preql --run`

  ```text
  Syntax error in answer_2306670096.preql: Undefined concept: local.company_claim_number (line 10, col 10, in ORDER BY). Suggestions: ['ca.Claim.company_claim_number', 'ca.Claim.company_subclaim_number']
  ```

### `join-resolution`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.claim_close_date, c.claim_identifier, c.claim_open_date}; {cc.Policy_Coverage_Detail.Policy.policy_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.claim_close_date, c.claim_identifier, c.claim_open_date}; {cc.Policy_Coverage_Detail.Policy.policy_number, cc.claim_identifier}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Resolution error in probe9.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.claim_identifier}; {pcd.Policy.policy_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {apr.agreement_identifier, apr.effective_date, apr.expiration_date, apr.party_identifier, apr.party_role_code}; {p.policy_number, p.status_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_dry.preql --run-and-delete`

  ```text
  Resolution error in probe_dry.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {apr.agreement_identifier, apr.party_identifier, apr.party_role_code}; {p.policy_identifier, p.policy_number, p.status_code}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | order by id;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...icy_amount_identifier as id;  ??? order by id;
  ```
- `trilogy file write probe_dry.preql --run-and-delete`

  ```text
  refused to write 'probe_dry.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...e apr.party_role_code = 'PH'  ??? subset join apr.agreement_iden...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:3
    |
  9 |   by cc.claim_identifier, cc.effective_date, cc.Policy_Coverage_Detail.policy_coverage_detail_identifier, cc.Policy_Coverage_Detail.Policy.policy_number
    |   ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...im_identifier) as n_claims    ??? by cc.claim_identifier, cc.eff...
  ```
