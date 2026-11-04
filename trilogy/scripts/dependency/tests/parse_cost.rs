// Parse cost grows linearly with expression nesting.
//
// pest has no memoization: an alternation whose later branch re-reads what an
// earlier one read parses that text twice, and in a nested expression the
// doubling compounds per level. `rule_calls` is the deterministic measure;
// `tests/parsing/test_parse_cost.py` holds the fuller sweep over the Python
// build, this is the gate that runs with the crate alone.

use trilogy_parser::rule_calls;

const MAX_CALLS: usize = 5_000_000;

type Wrap = fn(&str) -> String;

fn nested(wrap: Wrap, depth: usize) -> String {
    let mut expression = "x".to_string();
    for _ in 0..depth {
        expression = wrap(&expression);
    }
    format!("const x <- 1;\nauto y <- {expression};")
}

fn calls(text: &str) -> usize {
    rule_calls(text, MAX_CALLS)
        .expect("parses")
        .unwrap_or_else(|| panic!("more than {MAX_CALLS} rule calls"))
}

#[test]
fn nesting_cost_is_linear() {
    let shapes: [(&str, Wrap); 6] = [
        ("call", |e| format!("coalesce({e}, 1)")),
        ("aggregate", |e| format!("sum({e})")),
        ("aggregate_by", |e| format!("(avg({e}) by grp)")),
        ("aggregate_over", |e| format!("sum({e}) over (partition by g)")),
        ("filter", |e| format!("sum({e} ? y > 1)")),
        ("comparison", |e| format!("coalesce({e} = 1, true)")),
    ];
    for (name, wrap) in shapes {
        let shallow = calls(&nested(wrap, 8));
        let deep = calls(&nested(wrap, 16));
        assert!(
            deep as f64 <= 2.5 * shallow as f64,
            "{name}: {shallow} calls at depth 8, {deep} at depth 16"
        );
    }
}

#[test]
fn the_motivating_def_line_parses_in_one_pass() {
    // 7 s and hundreds of thousands of calls before the grammar was
    // left-factored; 10,584 calls when this was written.
    let line = "def fit_s(x, y, b, n, grp) -> case when n > 2 then sqrt(greatest(\
        n / (n - 2) * ((avg(y * y) by grp) - (avg(y) by grp) ** 2 - b * \
        ((avg(x * y) by grp) - (avg(x) by grp) * (avg(y) by grp))), 0.0)) \
        else null end;";
    assert!(calls(line) < 30_000);
}

#[test]
fn rule_calls_reports_the_limit_and_parse_errors() {
    assert_eq!(rule_calls("const x <- 1;", 3).expect("a parse error is not a limit"), None);
    assert!(rule_calls("const x <- ;", MAX_CALLS).is_err());
}
