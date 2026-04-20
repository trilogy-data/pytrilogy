use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use pyo3::prelude::*;
use pyo3::sync::GILProtected;
use pyo3::types::{PyList, PyString, PyTuple};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Parser)]
#[grammar = "trilogy.pest"]
pub struct TrilogyParser;

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
static RULE_INFO_CACHE: GILProtected<RefCell<Option<HashMap<Rule, RuleInfo>>>> =
    GILProtected::new(RefCell::new(None));

fn rule_info(py: Python<'_>, rule: Rule) -> (Py<PyString>, bool, bool) {
    let cell = RULE_INFO_CACHE.get(py);
    let mut borrowed = cell.borrow_mut();
    let map = borrowed.get_or_insert_with(HashMap::new);
    if let Some(info) = map.get(&rule) {
        return (info.name_py.clone_ref(py), info.is_token, info.is_silent);
    }
    let raw = format!("{:?}", rule);
    let renamed = rename_rule(&raw);
    let is_token = is_token_rule(&raw);
    let is_silent = is_token && is_silent_token_rule(&raw);
    let name_py = PyString::intern_bound(py, renamed).unbind();
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
static STATIC_STR_CACHE: GILProtected<RefCell<Option<HashMap<&'static str, Py<PyString>>>>> =
    GILProtected::new(RefCell::new(None));

fn intern_static(py: Python<'_>, s: &'static str) -> Py<PyString> {
    let cell = STATIC_STR_CACHE.get(py);
    let mut borrowed = cell.borrow_mut();
    let map = borrowed.get_or_insert_with(HashMap::new);
    if let Some(existing) = map.get(s) {
        return existing.clone_ref(py);
    }
    let py_str = PyString::intern_bound(py, s).unbind();
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

fn span_positions(pair: &Pair<Rule>, idx: &LineIndex) -> (usize, usize, usize, usize, usize, usize) {
    let span = pair.as_span();
    let (start_line, start_col, start_char) = idx.line_col(span.start());
    let (end_line, end_col, end_char) = idx.line_col(span.end());
    (start_line, start_col, end_line, end_col, start_char, end_char)
}

type SpanTuple = (usize, usize, usize, usize, usize, usize);

fn make_node(
    py: Python<'_>,
    name_py: Py<PyString>,
    list: Bound<PyList>,
    span: SpanTuple,
) -> PyResult<PyObject> {
    let (line, column, end_line, end_column, start_pos, end_pos) = span;
    let node = PestNode {
        data_py: name_py,
        children_list: list.unbind(),
        line,
        column,
        end_line,
        end_column,
        start_pos,
        end_pos,
    };
    Ok(Py::new(py, node)?.into_any())
}

// access_chain = _atom + (dot_tail | bracket_tail | dcolon_tail)*
// Left-factored to parse _atom once. Here we rewrite the node name and shape
// to match what the v2 hydrator (and lark grammar) expects.
fn convert_access_chain(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex) -> PyResult<PyObject> {
    let span = span_positions(&pair, idx);
    let mut inner = pair.into_inner();
    let atom_pair = inner
        .next()
        .expect("access_chain must contain an atom as first child");

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
        return convert_pair(py, atom_pair, idx);
    }

    let atom_py = convert_pair(py, atom_pair, idx)?;

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
        let list = PyList::empty_bound(py);
        list.append(atom_py)?;
        list.append(convert_pair(py, payload, idx)?)?;
        return make_node(py, intern_static(py, name), list, span);
    }

    if last_is_cast {
        let (_, cast_payload) = tails.pop().unwrap();
        let inner_name: &'static str = if tails.len() == 1 {
            let (k, p) = &tails[0];
            tail_name(*k, p)
        } else {
            "chained_access"
        };
        let inner_list = PyList::empty_bound(py);
        inner_list.append(atom_py)?;
        for (_, payload) in tails.into_iter() {
            inner_list.append(convert_pair(py, payload, idx)?)?;
        }
        let inner_node = make_node(py, intern_static(py, inner_name), inner_list, span)?;

        let outer_list = PyList::empty_bound(py);
        outer_list.append(inner_node)?;
        outer_list.append(convert_pair(py, cast_payload, idx)?)?;
        return make_node(py, intern_static(py, "fcast"), outer_list, span);
    }

    let list = PyList::empty_bound(py);
    list.append(atom_py)?;
    for (_, payload) in tails.into_iter() {
        list.append(convert_pair(py, payload, idx)?)?;
    }
    make_node(py, intern_static(py, "chained_access"), list, span)
}

fn convert_pair(py: Python<'_>, pair: Pair<Rule>, idx: &LineIndex) -> PyResult<PyObject> {
    let rule = pair.as_rule();
    if matches!(rule, Rule::access_chain) {
        return convert_access_chain(py, pair, idx);
    }

    let (name_py, is_token, _is_silent) = rule_info(py, rule);
    let span = span_positions(&pair, idx);
    let (line, column, end_line, end_column, start_pos, end_pos) = span;

    if is_token {
        let value_py = PyString::new_bound(py, pair.as_str()).unbind();
        let token = PestToken {
            type_name_py: name_py,
            value_py,
            line,
            column,
            end_line,
            end_column,
            start_pos,
            end_pos,
        };
        Ok(Py::new(py, token)?.into_any())
    } else {
        let list = PyList::empty_bound(py);
        for inner in pair.into_inner() {
            if matches!(inner.as_rule(), Rule::EOI) {
                continue;
            }
            let (_, _, inner_silent) = rule_info(py, inner.as_rule());
            if inner_silent {
                continue;
            }
            let child = convert_pair(py, inner, idx)?;
            list.append(child)?;
        }
        make_node(py, name_py, list, span)
    }
}

/// Parse a Trilogy source text and return a duck-typed syntax tree that
/// `trilogy.parsing.v2.syntax.syntax_from_parser` can consume unchanged.
#[pyfunction]
pub fn parse_trilogy_syntax(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let idx = LineIndex::new(text);
    let mut pairs = TrilogyParser::parse(Rule::start, text).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
    })?;
    let start = pairs
        .next()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("empty parse result"))?;
    convert_pair(py, start, &idx)
}

// -- Tuple-based representation: each node/token becomes a PyTuple, avoiding
// the pyclass allocation overhead of PestNode/PestToken. Layout:
//   node:  (name_py, children_py_tuple, line, col, end_line, end_col, start_pos, end_pos)
//   token: (name_py, value_py_string,   line, col, end_line, end_col, start_pos, end_pos)
// Nodes vs tokens are distinguished by whether element[1] is a tuple or str.

fn convert_access_chain_tuple<'py>(
    py: Python<'py>,
    pair: Pair<Rule>,
    idx: &LineIndex,
) -> PyResult<Bound<'py, PyTuple>> {
    let span = span_positions(&pair, idx);
    let mut inner = pair.into_inner();
    let atom_pair = inner
        .next()
        .expect("access_chain must contain an atom as first child");

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
        return convert_pair_tuple(py, atom_pair, idx);
    }

    let atom_tup = convert_pair_tuple(py, atom_pair, idx)?;

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
        let payload_tup = convert_pair_tuple(py, payload, idx)?;
        let children = PyTuple::new_bound(py, [atom_tup.into_any(), payload_tup.into_any()]);
        return make_node_tuple(py, intern_static(py, name), children, span);
    }

    if last_is_cast {
        let (_, cast_payload) = tails.pop().unwrap();
        let inner_name: &'static str = if tails.len() == 1 {
            let (k, p) = &tails[0];
            tail_name(*k, p)
        } else {
            "chained_access"
        };
        let mut inner_items: Vec<PyObject> = Vec::with_capacity(tails.len() + 1);
        inner_items.push(atom_tup.into_any().unbind());
        for (_, payload) in tails.into_iter() {
            inner_items.push(convert_pair_tuple(py, payload, idx)?.into_any().unbind());
        }
        let inner_children = PyTuple::new_bound(py, inner_items);
        let inner_node = make_node_tuple(py, intern_static(py, inner_name), inner_children, span)?;

        let cast_tup = convert_pair_tuple(py, cast_payload, idx)?;
        let outer_children = PyTuple::new_bound(py, [inner_node.into_any(), cast_tup.into_any()]);
        return make_node_tuple(py, intern_static(py, "fcast"), outer_children, span);
    }

    let mut items: Vec<PyObject> = Vec::with_capacity(tails.len() + 1);
    items.push(atom_tup.into_any().unbind());
    for (_, payload) in tails.into_iter() {
        items.push(convert_pair_tuple(py, payload, idx)?.into_any().unbind());
    }
    let children = PyTuple::new_bound(py, items);
    make_node_tuple(py, intern_static(py, "chained_access"), children, span)
}

fn make_node_tuple<'py>(
    py: Python<'py>,
    name_py: Py<PyString>,
    children: Bound<'py, PyTuple>,
    span: SpanTuple,
) -> PyResult<Bound<'py, PyTuple>> {
    let (line, column, end_line, end_column, start_pos, end_pos) = span;
    Ok(PyTuple::new_bound(
        py,
        [
            name_py.into_any(),
            children.into_any().unbind(),
            line.into_py(py),
            column.into_py(py),
            end_line.into_py(py),
            end_column.into_py(py),
            start_pos.into_py(py),
            end_pos.into_py(py),
        ],
    ))
}

fn convert_pair_tuple<'py>(
    py: Python<'py>,
    pair: Pair<Rule>,
    idx: &LineIndex,
) -> PyResult<Bound<'py, PyTuple>> {
    let rule = pair.as_rule();
    if matches!(rule, Rule::access_chain) {
        return convert_access_chain_tuple(py, pair, idx);
    }

    let (name_py, is_token, _is_silent) = rule_info(py, rule);
    let span = span_positions(&pair, idx);
    let (line, column, end_line, end_column, start_pos, end_pos) = span;

    if is_token {
        let value_py = PyString::new_bound(py, pair.as_str()).unbind();
        Ok(PyTuple::new_bound(
            py,
            [
                name_py.into_any(),
                value_py.into_any(),
                line.into_py(py),
                column.into_py(py),
                end_line.into_py(py),
                end_column.into_py(py),
                start_pos.into_py(py),
                end_pos.into_py(py),
            ],
        ))
    } else {
        let mut items: Vec<PyObject> = Vec::new();
        for inner in pair.into_inner() {
            if matches!(inner.as_rule(), Rule::EOI) {
                continue;
            }
            let (_, _, inner_silent) = rule_info(py, inner.as_rule());
            if inner_silent {
                continue;
            }
            items.push(convert_pair_tuple(py, inner, idx)?.into_any().unbind());
        }
        let children = PyTuple::new_bound(py, items);
        make_node_tuple(py, name_py, children, span)
    }
}

/// Tuple-based parse: returns a nested PyTuple tree. Layout described above.
#[pyfunction]
pub fn parse_trilogy_syntax_tuple(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let idx = LineIndex::new(text);
    let mut pairs = TrilogyParser::parse(Rule::start, text).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
    })?;
    let start = pairs
        .next()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("empty parse result"))?;
    Ok(convert_pair_tuple(py, start, &idx)?.into_any().unbind())
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
