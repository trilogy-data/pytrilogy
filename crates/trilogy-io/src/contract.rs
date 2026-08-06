//! What Trilogy may ask a source for, and how that ask is honored.
//!
//! Rust has no signature introspection, so a source declares what it pushes
//! down with [`Source::pushdown`](crate::Source::pushdown). Everything it does
//! not claim is enforced on the output stream by [`apply`], exactly as the
//! python wrapper does -- the contract is always honored either way.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;

use crate::error::{Error, Result};

pub const CONTRACT_VERSION: u32 = 1;

/// A contract field a source can take ownership of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Field {
    Limit,
    Columns,
    Filters,
    OrderBy,
    Since,
    Partition,
}

impl Field {
    pub fn as_str(&self) -> &'static str {
        match self {
            Field::Limit => "limit",
            Field::Columns => "columns",
            Field::Filters => "filters",
            Field::OrderBy => "order_by",
            Field::Since => "since",
            Field::Partition => "partition",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    In,
    NotIn,
    Like,
}

impl Op {
    fn parse(raw: &str) -> Option<Op> {
        let normalized = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        Some(match normalized.to_ascii_lowercase().as_str() {
            "=" => Op::Eq,
            "!=" => Op::NotEq,
            "<" => Op::Lt,
            "<=" => Op::LtEq,
            ">" => Op::Gt,
            ">=" => Op::GtEq,
            "in" => Op::In,
            "not in" => Op::NotIn,
            "like" => Op::Like,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::NotEq => "!=",
            Op::Lt => "<",
            Op::LtEq => "<=",
            Op::Gt => ">",
            Op::GtEq => ">=",
            Op::In => "in",
            Op::NotIn => "not in",
            Op::Like => "like",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub column: String,
    pub op: Op,
    pub value: Value,
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.column, self.op.as_str(), self.value)
    }
}

impl Filter {
    /// Parses `<column> <op> <value>`, the same wire form the python side reads.
    ///
    /// The value is JSON first -- so numbers, lists and null work -- and a bare
    /// word falls back to a string.
    pub fn parse(text: &str) -> Result<Filter> {
        let text = text.trim();
        let (column, rest) = split_identifier(text)
            .ok_or_else(|| Error::contract(format!("Could not parse filter {text:?}")))?;
        let (op_text, value_text) = split_operator(rest)
            .ok_or_else(|| Error::contract(format!("Could not parse filter {text:?}")))?;
        let op = Op::parse(op_text)
            .ok_or_else(|| Error::contract(format!("Unsupported filter operator {op_text:?}")))?;
        Ok(Filter {
            column: column.to_string(),
            op,
            value: parse_value(value_text.trim()),
        })
    }
}

fn split_identifier(text: &str) -> Option<(&str, &str)> {
    let end = text
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
        .unwrap_or(text.len());
    if end == 0 {
        return None;
    }
    let (column, rest) = text.split_at(end);
    if !column.starts_with(|c: char| c.is_alphabetic() || c == '_') {
        return None;
    }
    Some((column, rest))
}

fn split_operator(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_start();
    let lowered = text.to_ascii_lowercase();
    // Longest first, so `<=` never matches as `<` and `not in` never as `in`.
    for candidate in ["not in", ">=", "<=", "!=", "like", "in", "=", ">", "<"] {
        if lowered.starts_with(candidate) {
            let (op, rest) = text.split_at(candidate.len());
            // A word operator must be followed by a break, or `internal = 1`
            // would parse its column as `int` with operator `in`.
            if candidate.chars().next().is_some_and(|c| c.is_alphabetic())
                && !rest.starts_with(|c: char| c.is_whitespace() || c == '[' || c == '(')
            {
                continue;
            }
            return Some((op, rest));
        }
    }
    None
}

fn parse_value(raw: &str) -> Value {
    serde_json::from_str(raw)
        .unwrap_or_else(|_| Value::String(raw.trim_matches(['\'', '"']).to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sort {
    pub column: String,
    pub descending: bool,
}

impl Sort {
    /// Parses `<column>` or `<column>:asc` / `<column>:desc`.
    pub fn parse(text: &str) -> Result<Sort> {
        let (column, direction) = match text.split_once(':') {
            Some((c, d)) => (c, d.trim().to_ascii_lowercase()),
            None => (text, "asc".to_string()),
        };
        let descending = match direction.as_str() {
            "asc" => false,
            "desc" => true,
            _ => {
                return Err(Error::contract(format!(
                    "Could not parse sort {text:?}. Expected '<column>' or '<column>:asc' / '<column>:desc'."
                )))
            }
        };
        Ok(Sort {
            column: column.trim().to_string(),
            descending,
        })
    }

    pub fn render(&self) -> String {
        format!(
            "{}:{}",
            self.column,
            if self.descending { "desc" } else { "asc" }
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceRequest {
    pub limit: Option<usize>,
    pub columns: Option<Vec<String>>,
    pub filters: Vec<Filter>,
    pub order_by: Vec<Sort>,
    pub since: Option<String>,
    pub partition: BTreeMap<String, String>,
}

impl SourceRequest {
    fn is_set(&self, field: Field) -> bool {
        match field {
            Field::Limit => self.limit.is_some(),
            Field::Columns => self.columns.is_some(),
            Field::Filters => !self.filters.is_empty(),
            Field::OrderBy => !self.order_by.is_empty(),
            Field::Since => self.since.is_some(),
            Field::Partition => !self.partition.is_empty(),
        }
    }

    fn clear(&mut self, field: Field) {
        match field {
            Field::Limit => self.limit = None,
            Field::Columns => self.columns = None,
            Field::Filters => self.filters.clear(),
            Field::OrderBy => self.order_by.clear(),
            Field::Since => self.since = None,
            Field::Partition => self.partition.clear(),
        }
    }

    /// The request as handed to a source: fields it does not own are blanked,
    /// so the fallback stays authoritative.
    pub fn withheld(&self, pushdown: &[Field]) -> SourceRequest {
        let mut request = self.clone();
        for field in [
            Field::Limit,
            Field::Columns,
            Field::Filters,
            Field::OrderBy,
            Field::Since,
            Field::Partition,
        ] {
            if !pushdown.contains(&field) {
                request.clear(field);
            }
        }
        request
    }
}

/// Narrow a declared pushdown to what is safe to hand over for this request.
///
/// `limit` only composes if it is applied last. A source that truncates to
/// `limit` rows and then has a filter applied -- or has its output sorted --
/// returns the wrong rows, so whenever narrowing or ordering is still enforced
/// locally the limit comes back to the fallback with it.
pub fn effective_pushdown(declared: &[Field], request: &SourceRequest) -> Vec<Field> {
    let local = |field: Field| request.is_set(field) && !declared.contains(&field);
    let narrowing_is_local = local(Field::Filters) || local(Field::OrderBy);
    declared
        .iter()
        .copied()
        .filter(|field| !(narrowing_is_local && *field == Field::Limit))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_operators_and_values() {
        assert_eq!(
            Filter::parse("i >= 5").unwrap(),
            Filter {
                column: "i".into(),
                op: Op::GtEq,
                value: Value::from(5)
            }
        );
        assert_eq!(
            Filter::parse("state = CA").unwrap().value,
            Value::from("CA")
        );
        assert_eq!(
            Filter::parse(r#"state = "CA""#).unwrap().value,
            Value::from("CA")
        );
        assert_eq!(Filter::parse("s NOT IN [1]").unwrap().op, Op::NotIn);
        assert_eq!(Filter::parse("s like n%").unwrap().value, Value::from("n%"));
        assert_eq!(
            Filter::parse(r#"state in ["CA","NY"]"#).unwrap().value,
            serde_json::json!(["CA", "NY"])
        );
    }

    #[test]
    fn a_column_starting_with_an_operator_word_still_parses() {
        let filter = Filter::parse("internal = 1").unwrap();
        assert_eq!(filter.column, "internal");
        assert_eq!(filter.op, Op::Eq);
    }

    #[test]
    fn rejects_garbage() {
        assert!(Filter::parse("this is not a filter").is_err());
        assert!(Filter::parse("").is_err());
    }

    #[test]
    fn limit_pushdown_is_withdrawn_when_filters_stay_local() {
        let request = SourceRequest {
            limit: Some(4),
            filters: vec![Filter::parse("state = CA").unwrap()],
            ..Default::default()
        };
        assert_eq!(effective_pushdown(&[Field::Limit], &request), vec![]);
        assert_eq!(
            effective_pushdown(&[Field::Limit, Field::Filters], &request),
            vec![Field::Limit, Field::Filters]
        );
        let plain = SourceRequest {
            limit: Some(4),
            ..Default::default()
        };
        assert_eq!(
            effective_pushdown(&[Field::Limit], &plain),
            vec![Field::Limit]
        );
    }

    #[test]
    fn parses_sorts() {
        assert_eq!(
            Sort::parse("id:desc").unwrap(),
            Sort {
                column: "id".into(),
                descending: true
            }
        );
        assert_eq!(
            Sort::parse("id").unwrap(),
            Sort {
                column: "id".into(),
                descending: false
            }
        );
        assert!(Sort::parse("id:sideways").is_err());
        assert_eq!(Sort::parse("id:desc").unwrap().render(), "id:desc");
    }

    #[test]
    fn a_local_ordering_also_keeps_the_limit_home() {
        let request = SourceRequest {
            limit: Some(4),
            order_by: vec![Sort {
                column: "id".into(),
                descending: true,
            }],
            ..Default::default()
        };
        assert_eq!(effective_pushdown(&[Field::Limit], &request), vec![]);
        assert_eq!(
            effective_pushdown(&[Field::Limit, Field::OrderBy], &request),
            vec![Field::Limit, Field::OrderBy]
        );
    }

    #[test]
    fn withheld_blanks_unowned_fields() {
        let request = SourceRequest {
            limit: Some(4),
            filters: vec![Filter::parse("state = CA").unwrap()],
            ..Default::default()
        };
        let handed = request.withheld(&[Field::Filters]);
        assert!(handed.limit.is_none());
        assert_eq!(handed.filters.len(), 1);
    }
}
