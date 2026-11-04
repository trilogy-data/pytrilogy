use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "trilogy.pest"]
pub struct TrilogyParser;

/// How many rule calls a parse of `text` makes, or `None` when it makes more
/// than `max_calls`. Parse errors other than the call limit are returned.
///
/// The count is the gate on parse cost: it is deterministic and the same on
/// every machine, where a timing is neither. pest exposes it only as a limit
/// (`set_call_limit`, a process-wide setting), so the count is the smallest
/// limit the parse completes under, found by bisection; every probe stops at
/// its limit, so a pathological input costs at most `max_calls` calls per
/// probe rather than running to completion. Probes are serialized because
/// the limit is global, and it is cleared before this returns.
pub fn rule_calls(text: &str, max_calls: usize) -> Result<Option<usize>, Box<pest::error::Error<Rule>>> {
    use pest::error::ErrorVariant;
    use pest::Parser;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;

    static PROBE: Mutex<()> = Mutex::new(());
    let _probe = PROBE.lock().unwrap_or_else(|e| e.into_inner());

    let completes_within = |limit: usize| -> Result<bool, Box<pest::error::Error<Rule>>> {
        pest::set_call_limit(NonZeroUsize::new(limit));
        let result = TrilogyParser::parse(Rule::start, text);
        pest::set_call_limit(None);
        match result {
            Ok(_) => Ok(true),
            Err(e) => match &e.variant {
                ErrorVariant::CustomError { message } if message == "call limit reached" => Ok(false),
                _ => Err(Box::new(e)),
            },
        }
    };
    if !completes_within(max_calls)? {
        return Ok(None);
    }
    let (mut low, mut high) = (1usize, max_calls);
    while low < high {
        let mid = low + (high - low) / 2;
        if completes_within(mid)? {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(Some(low))
}

// Everything below is the PyO3 conversion layer over the pest tree and only
// exists in `python`-feature builds. `TrilogyParser`/`Rule` above stay
// available to pure-Rust consumers (the CLI, downstream crates).
#[cfg(feature = "python")]
mod python_impl {
use super::{Rule, TrilogyParser};
use pest::iterators::Pair;
use pest::Parser;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};
use std::collections::HashMap;
use std::sync::Mutex;

fn rename_rule(name: &str) -> &str {
    match name {
        "attr_access_paren" => "attr_access",
        "_and_conditional" => "conditional",
        other => other,
    }
}

fn is_token_rule(name: &str) -> bool {
    name.chars()
        .find(|c| c.is_alphabetic())
        .map(|c| c.is_ascii_uppercase())
        .unwrap_or(false)
}

// Atomic rules whose name starts with `_` are silent-atomic helpers used only
// for matching (e.g. multi-word keywords like `def table`). They should not
// appear in the v2 syntax tree.
fn is_silent_token_rule(name: &str) -> bool {
    name.starts_with('_')
}

struct RuleInfo {
    name_py: Py<PyString>,
    is_token: bool,
    is_silent: bool,
}

// Per-Rule cache of (interned Py<PyString> name, is_token flag, is_silent flag).
// Populated lazily — amortizes format! + rename_rule to once per unique rule.
static RULE_INFO_CACHE: Mutex<Option<HashMap<Rule, RuleInfo>>> = Mutex::new(None);

fn rule_info(py: Python<'_>, rule: Rule) -> (Py<PyString>, bool, bool) {
    let mut borrowed = RULE_INFO_CACHE.lock().unwrap();
    let map = borrowed.get_or_insert_with(HashMap::new);
    if let Some(info) = map.get(&rule) {
        return (info.name_py.clone_ref(py), info.is_token, info.is_silent);
    }
    let raw = format!("{:?}", rule);
    let renamed = rename_rule(&raw);
    let is_token = is_token_rule(&raw);
    let is_silent = is_token && is_silent_token_rule(&raw);
    let name_py = PyString::intern(py, renamed).unbind();
    let cloned = name_py.clone_ref(py);
    map.insert(
        rule,
        RuleInfo {
            name_py,
            is_token,
            is_silent,
        },
    );
    (cloned, is_token, is_silent)
}

// Interns a short `&'static str` as a cached Py<PyString>. Used for access_chain
// rewrites that synthesize names not present in the grammar.
static STATIC_STR_CACHE: Mutex<Option<HashMap<&'static str, Py<PyString>>>> = Mutex::new(None);

fn intern_static(py: Python<'_>, s: &'static str) -> Py<PyString> {
    let mut borrowed = STATIC_STR_CACHE.lock().unwrap();
    let map = borrowed.get_or_insert_with(HashMap::new);
    if let Some(existing) = map.get(s) {
        return existing.clone_ref(py);
    }
    let py_str = PyString::intern(py, s).unbind();
    map.insert(s, py_str.clone_ref(py));
    py_str
}

#[pyclass(module = "_preql_import_resolver")]
pub struct PestNode {
    data_py: Py<PyString>,
    children_list: Py<PyList>,
    #[pyo3(get)]
    pub line: usize,
    #[pyo3(get)]
    pub column: usize,
    #[pyo3(get)]
    pub end_line: usize,
    #[pyo3(get)]
    pub end_column: usize,
    #[pyo3(get)]
    pub start_pos: usize,
    #[pyo3(get)]
    pub end_pos: usize,
}

#[pymethods]
impl PestNode {
    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyString> {
        self.data_py.clone_ref(py)
    }

    #[getter]
    fn children<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        self.children_list.bind(py).clone()
    }

    // `meta` returns self so that `SyntaxMeta.from_parser_meta` reads
    // line/column/etc. directly off the node — one fewer Python allocation per node.
    #[getter]
    fn meta(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!("PestNode({})", self.data_py.bind(py).to_string_lossy())
    }
}

#[pyclass(module = "_preql_import_resolver")]
pub struct PestToken {
    type_name_py: Py<PyString>,
    value_py: Py<PyString>,
    #[pyo3(get)]
    pub line: usize,
    #[pyo3(get)]
    pub column: usize,
    #[pyo3(get)]
    pub end_line: usize,
    #[pyo3(get)]
    pub end_column: usize,
    #[pyo3(get)]
    pub start_pos: usize,
    #[pyo3(get)]
    pub end_pos: usize,
}

#[pymethods]
impl PestToken {
    #[getter]
    #[pyo3(name = "type")]
    fn type_name(&self, py: Python<'_>) -> Py<PyString> {
        self.type_name_py.clone_ref(py)
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> Py<PyString> {
        self.value_py.clone_ref(py)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "PestToken({}={:?})",
            self.type_name_py.bind(py).to_string_lossy(),
            self.value_py.bind(py).to_string_lossy()
        )
    }

    fn __str__(&self, py: Python<'_>) -> String {
        self.value_py.bind(py).to_string_lossy().into_owned()
    }
}

// Precomputed mapping from pest byte offsets to Python char offsets, plus
// line-start char positions for O(log n) line/col lookup. Pest gives us byte
// offsets in the UTF-8 source, but Python str indexing is char-based — for
// multibyte source (non-ASCII identifiers, strings) the two diverge and the
// downstream hydrator gets nonsensical positions.
struct LineIndex {
    // Byte offset of each Unicode scalar value, plus a final sentinel == text.len().
    // Sorted ascending; binary_search converts byte → char index.
    char_byte_starts: Vec<usize>,
    // Char positions of each line start (0, then char-after each '\n').
    line_starts_char: Vec<usize>,
}

impl LineIndex {
    fn new(text: &str) -> Self {
        let mut char_byte_starts = Vec::with_capacity(text.len() + 1);
        let mut line_starts_char = Vec::with_capacity(text.len() / 40 + 1);
        line_starts_char.push(0);
        let mut char_idx = 0usize;
        for (byte_offset, ch) in text.char_indices() {
            char_byte_starts.push(byte_offset);
            if ch == '\n' {
                line_starts_char.push(char_idx + 1);
            }
            char_idx += 1;
        }
        char_byte_starts.push(text.len());
        Self {
            char_byte_starts,
            line_starts_char,
        }
    }

    fn byte_to_char(&self, byte_pos: usize) -> usize {
        match self.char_byte_starts.binary_search(&byte_pos) {
            Ok(i) => i,
            Err(i) => i - 1,
        }
    }

    // Returns (line, col, char_pos) — line/col are 1-based, char_pos is 0-based.
    fn line_col(&self, byte_pos: usize) -> (usize, usize, usize) {
        let char_pos = self.byte_to_char(byte_pos);
        let idx = match self.line_starts_char.binary_search(&char_pos) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let line_start = self.line_starts_char[idx];
        (idx + 1, char_pos - line_start + 1, char_pos)
    }
}

type SpanTuple = (usize, usize, usize, usize, usize, usize);

fn span_positions(pair: &Pair<Rule>, idx: &LineIndex) -> SpanTuple {
    let span = pair.as_span();
    span_from_to(span.start(), span.end(), idx)
}

// The two output representations (PestNode/PestToken pyclasses, and the
// tuple layout `parse_trilogy_syntax_tuple` returns) share one walk over the
// pest tree, so every shape rewrite below is written once.
trait TreeBuilder {
    fn node(&self, py: Python<'_>, name: Py<PyString>, children: Vec<Py<PyAny>>, span: SpanTuple) -> PyResult<Py<PyAny>>;
    fn token(&self, py: Python<'_>, name: Py<PyString>, value: &str, span: SpanTuple) -> PyResult<Py<PyAny>>;
}

struct ClassBuilder;

impl TreeBuilder for ClassBuilder {
    fn node(&self, py: Python<'_>, name: Py<PyString>, children: Vec<Py<PyAny>>, span: SpanTuple) -> PyResult<Py<PyAny>> {
        let (line, column, end_line, end_column, start_pos, end_pos) = span;
        let node = PestNode {
            data_py: name,
            children_list: PyList::new(py, children)?.unbind(),
            line,
            column,
            end_line,
            end_column,
            start_pos,
            end_pos,
        };
        Ok(Py::new(py, node)?.into_any())
    }

    fn token(&self, py: Python<'_>, name: Py<PyString>, value: &str, span: SpanTuple) -> PyResult<Py<PyAny>> {
        let (line, column, end_line, end_column, start_pos, end_pos) = span;
        let token = PestToken {
            type_name_py: name,
            value_py: PyString::new(py, value).unbind(),
            line,
            column,
            end_line,
            end_column,
            start_pos,
            end_pos,
        };
        Ok(Py::new(py, token)?.into_any())
    }
}

// Tuple-based representation: each node/token becomes a PyTuple, avoiding
// the pyclass allocation overhead of PestNode/PestToken. Layout:
//   node:  (name_py, children_py_tuple, line, col, end_line, end_col, start_pos, end_pos)
//   token: (name_py, value_py_string,   line, col, end_line, end_col, start_pos, end_pos)
// Nodes vs tokens are distinguished by whether element[1] is a tuple or str.
struct TupleBuilder;

impl TreeBuilder for TupleBuilder {
    fn node(&self, py: Python<'_>, name: Py<PyString>, children: Vec<Py<PyAny>>, span: SpanTuple) -> PyResult<Py<PyAny>> {
        let (line, column, end_line, end_column, start_pos, end_pos) = span;
        let children = PyTuple::new(py, children)?;
        Ok((name, children, line, column, end_line, end_column, start_pos, end_pos)
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }

    fn token(&self, py: Python<'_>, name: Py<PyString>, value: &str, span: SpanTuple) -> PyResult<Py<PyAny>> {
        let (line, column, end_line, end_column, start_pos, end_pos) = span;
        let value_py = PyString::new(py, value).unbind();
        Ok((name, value_py, line, column, end_line, end_column, start_pos, end_pos)
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }
}

fn span_from_to(start_byte: usize, end_byte: usize, idx: &LineIndex) -> SpanTuple {
    let (start_line, start_col, start_char) = idx.line_col(start_byte);
    let (end_line, end_col, end_char) = idx.line_col(end_byte);
    (start_line, start_col, end_line, end_col, start_char, end_char)
}

// The pairs the adapter sees: pest drops silent rules, and the silent-atomic
// helpers (`_DEF_TABLE`) and EOI are dropped here.
fn visible_children(pair: Pair<Rule>) -> Vec<Pair<Rule>> {
    pair.into_inner()
        .filter(|inner| {
            let name = format!("{:?}", inner.as_rule());
            !matches!(inner.as_rule(), Rule::EOI) && !(is_token_rule(&name) && is_silent_token_rule(&name))
        })
        .collect()
}

fn convert_children<B: TreeBuilder>(py: Python<'_>, pairs: Vec<Pair<Rule>>, idx: &LineIndex, b: &B) -> PyResult<Vec<Py<PyAny>>> {
    pairs.into_iter().map(|p| convert_pair(py, p, idx, b)).collect()
}

// paren_group = "(" ~ expr ~ ("," ~ expr)* ~ ","? ~ ")"
// A single expr with no comma is lark's `parenthetical`; anything else its
// `expr_tuple`. The one-tuple `(x,)` is told from `(x)` by the comma in the
// gap between the expr and `)`, which may also hold whitespace and comments.
fn paren_group_is_tuple(pair: &Pair<Rule>) -> bool {
    let exprs: Vec<Pair<Rule>> = pair.clone().into_inner().collect();
    if exprs.len() != 1 {
        return true;
    }
    let text = pair.as_str();
    let gap = &text[exprs[0].as_span().end() - pair.as_span().start()..];
    let bytes = gap.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b',' => return true,
            b'#' => while i < bytes.len() && bytes[i] != b'\n' { i += 1 },
            b'/' if bytes.get(i + 1) == Some(&b'/') => while i < bytes.len() && bytes[i] != b'\n' { i += 1 },
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') { i += 1 }
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    false
}

fn convert_paren_group<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    let span = span_positions(&pair, idx);
    let name = if paren_group_is_tuple(&pair) { "expr_tuple" } else { "parenthetical" };
    let items = convert_children(py, visible_children(pair), idx, b)?;
    b.node(py, intern_static(py, name), items, span)
}

// access_chain = (_unfilterable_atom ~ _access_tail*) | (_atom ~ (("?" ~ conditional) | _access_tail*))
// Left-factored to parse the atom once. Here we rewrite the node name and shape
// to match what the v2 hydrator (and lark grammar) expects.
fn convert_access_chain<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    let span = span_positions(&pair, idx);
    let mut inner = pair.into_inner();
    let atom_pair = inner
        .next()
        .expect("access_chain must contain an atom as first child");

    // `X ? cond` is lark's `_filter_alt`: a filter_item over the bare head,
    // so a parenthesized head contributes its expr and a concept reference
    // its IDENTIFIER token.
    let mut inner = inner.peekable();
    if matches!(inner.peek().map(|p| p.as_rule()), Some(Rule::conditional)) {
        let conditional = inner.next().unwrap();
        let bare = match atom_pair.as_rule() {
            Rule::concept_lit => true,
            Rule::paren_group => !paren_group_is_tuple(&atom_pair),
            _ => false,
        };
        let head_pair = if bare {
            atom_pair
                .into_inner()
                .next()
                .expect("a parenthetical and a concept_lit each hold one child")
        } else {
            atom_pair
        };
        let head = convert_pair(py, head_pair, idx, b)?;
        let cond = convert_pair(py, conditional, idx, b)?;
        return b.node(py, intern_static(py, "filter_item"), vec![head, cond], span);
    }

    let mut tails: Vec<(Rule, Pair<Rule>)> = Vec::new();
    for tail in inner {
        let kind = tail.as_rule();
        let payload = tail
            .into_inner()
            .next()
            .expect("tail rule must have one payload child");
        tails.push((kind, payload));
    }

    if tails.is_empty() {
        return convert_pair(py, atom_pair, idx, b);
    }

    let atom_py = convert_pair(py, atom_pair, idx, b)?;

    let tail_name = |kind: Rule, payload: &Pair<Rule>| -> &'static str {
        match kind {
            Rule::dot_tail => "attr_access",
            Rule::bracket_tail => match payload.as_rule() {
                Rule::int_lit => "index_access",
                _ => "map_key_access",
            },
            Rule::dcolon_tail => "fcast",
            _ => "chained_access",
        }
    };

    let last_is_cast = matches!(tails.last().map(|(k, _)| *k), Some(Rule::dcolon_tail));

    if tails.len() == 1 {
        let (kind, payload) = tails.pop().unwrap();
        let name = tail_name(kind, &payload);
        let payload_py = convert_pair(py, payload, idx, b)?;
        return b.node(py, intern_static(py, name), vec![atom_py, payload_py], span);
    }

    if last_is_cast {
        let (_, cast_payload) = tails.pop().unwrap();
        let inner_name: &'static str = if tails.len() == 1 {
            let (k, p) = &tails[0];
            tail_name(*k, p)
        } else {
            "chained_access"
        };
        let mut inner_items: Vec<Py<PyAny>> = Vec::with_capacity(tails.len() + 1);
        inner_items.push(atom_py);
        for (_, payload) in tails.into_iter() {
            inner_items.push(convert_pair(py, payload, idx, b)?);
        }
        let inner_node = b.node(py, intern_static(py, inner_name), inner_items, span)?;
        let cast_py = convert_pair(py, cast_payload, idx, b)?;
        return b.node(py, intern_static(py, "fcast"), vec![inner_node, cast_py], span);
    }

    let mut items: Vec<Py<PyAny>> = Vec::with_capacity(tails.len() + 1);
    items.push(atom_py);
    for (_, payload) in tails.into_iter() {
        items.push(convert_pair(py, payload, idx, b)?);
    }
    b.node(py, intern_static(py, "chained_access"), items, span)
}

// expr = sum_operator ~ (_between_tail | _comparison_tail)?
// Left-factored to parse the head once; the children say which lark rule it
// is: [head] or [head, COMPARISON_OPERATOR, rhs] is a `comparison`,
// [head, low, high] a `between_comparison`.
fn convert_expr<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    let span = span_positions(&pair, idx);
    let children = visible_children(pair);
    let is_between = children.len() == 3 && matches!(children[1].as_rule(), Rule::sum_operator);
    let name = if is_between { "between_comparison" } else { "comparison" };
    let items = convert_children(py, children, idx, b)?;
    b.node(py, intern_static(py, name), items, span)
}

// select_chain = select_statement ~ _multi_select_tail?
// A chain with no tail is its select_statement; one with a `merge` tail is
// the multi_select_statement whose children follow.
fn convert_select_chain<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    let span = span_positions(&pair, idx);
    let mut children = visible_children(pair);
    if children.len() == 1 {
        return convert_pair(py, children.pop().unwrap(), idx, b);
    }
    let items = convert_children(py, children, idx, b)?;
    b.node(py, intern_static(py, "multi_select_statement"), items, span)
}

// aggregate_functions = _call ~ aggregate_over? ~ window_sql_over?
// With the `over (...)` tail the call is lark's aggregate-as-window:
// window_item(window_item_sql_aggregate(aggregate_functions, window_sql_over)).
// The inner aggregate_functions keeps the span the rule had before the tail
// joined it (`_call ~ aggregate_over?`): through its `by` clause when it has
// one, else through the whitespace pest skipped before declining that clause.
fn convert_aggregate_functions<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    let (start_byte, end_byte) = (pair.as_span().start(), pair.as_span().end());
    let (name_py, _, _) = rule_info(py, pair.as_rule());
    let mut children = visible_children(pair);
    let over_pair = match children.last().map(|c| c.as_rule()) {
        Some(Rule::window_sql_over) => children.pop(),
        _ => None,
    };
    let call_end = match (children.last(), &over_pair) {
        (Some(last), _) if matches!(last.as_rule(), Rule::aggregate_over) => last.as_span().end(),
        (_, Some(over)) => over.as_span().start(),
        _ => end_byte,
    };
    let call_items = convert_children(py, children, idx, b)?;
    let call = b.node(py, name_py, call_items, span_from_to(start_byte, call_end, idx))?;
    let Some(over_pair) = over_pair else {
        return Ok(call);
    };
    let span = span_from_to(start_byte, end_byte, idx);
    let over = convert_pair(py, over_pair, idx, b)?;
    let windowed = b.node(py, intern_static(py, "window_item_sql_aggregate"), vec![call, over], span)?;
    b.node(py, intern_static(py, "window_item"), vec![windowed], span)
}

fn convert_pair<B: TreeBuilder>(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex, b: &B) -> PyResult<Py<PyAny>> {
    match pair.as_rule() {
        Rule::access_chain => return convert_access_chain(py, pair, idx, b),
        Rule::expr => return convert_expr(py, pair, idx, b),
        Rule::select_chain => return convert_select_chain(py, pair, idx, b),
        Rule::aggregate_functions => return convert_aggregate_functions(py, pair, idx, b),
        Rule::paren_group => return convert_paren_group(py, pair, idx, b),
        _ => {}
    }

    let (name_py, is_token, _is_silent) = rule_info(py, pair.as_rule());
    let span = span_positions(&pair, idx);

    if is_token {
        return b.token(py, name_py, pair.as_str(), span);
    }
    let mut items: Vec<Py<PyAny>> = Vec::new();
    for inner in pair.into_inner() {
        if matches!(inner.as_rule(), Rule::EOI) {
            continue;
        }
        let (_, _, inner_silent) = rule_info(py, inner.as_rule());
        if inner_silent {
            continue;
        }
        items.push(convert_pair(py, inner, idx, b)?);
    }
    b.node(py, name_py, items, span)
}

fn parse_root<'i>(text: &'i str) -> PyResult<Pair<'i, Rule>> {
    let mut pairs = TrilogyParser::parse(Rule::start, text).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
    })?;
    pairs
        .next()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("empty parse result"))
}

/// Parse a Trilogy source text and return a duck-typed syntax tree that
/// `trilogy.parsing.v2.syntax.syntax_from_parser` can consume unchanged.
#[pyfunction]
pub fn parse_trilogy_syntax(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let idx = LineIndex::new(text);
    let start = parse_root(text)?;
    convert_pair(py, start, &idx, &ClassBuilder)
}

/// Tuple-based parse: returns a nested PyTuple tree. Layout at `TupleBuilder`.
#[pyfunction]
pub fn parse_trilogy_syntax_tuple(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let idx = LineIndex::new(text);
    let start = parse_root(text)?;
    convert_pair(py, start, &idx, &TupleBuilder)
}

/// The number of pest rule calls a parse of `text` makes (see `rule_calls`),
/// the deterministic measure the parse-cost tests gate on. Raises when the
/// parse needs more than `max_calls` calls or does not parse at all.
#[pyfunction]
pub fn parse_trilogy_syntax_calls(text: &str, max_calls: usize) -> PyResult<usize> {
    match super::rule_calls(text, max_calls) {
        Ok(Some(calls)) => Ok(calls),
        Ok(None) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "parse needs more than {max_calls} rule calls"
        ))),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))),
    }
}

/// Parse-only benchmark helper: runs the pest parser and walks the pair tree
/// to count nodes, but does NOT construct any Python objects. Useful to
/// separate raw pest cost from PyO3 conversion overhead.
#[pyfunction]
pub fn parse_trilogy_syntax_count(text: &str) -> PyResult<usize> {
    let mut pairs = TrilogyParser::parse(Rule::start, text).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
    })?;
    let start = pairs
        .next()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("empty parse result"))?;
    fn walk(pair: Pair<Rule>) -> usize {
        let mut n = 1;
        for inner in pair.into_inner() {
            n += walk(inner);
        }
        n
    }
    Ok(walk(start))
}
}

#[cfg(feature = "python")]
pub use python_impl::{
    parse_trilogy_syntax, parse_trilogy_syntax_calls, parse_trilogy_syntax_count,
    parse_trilogy_syntax_tuple, PestNode, PestToken,
};
