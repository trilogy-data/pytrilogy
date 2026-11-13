// Deeply nested input fails as an ordinary parse error instead of exhausting
// the native stack (trilogy-data/pytrilogy#946).

use pest::error::ErrorVariant;
use trilogy_parser::{parse_bounded, parse_file, ParseError, NESTING_LIMIT_MESSAGE};

// (name, head, open, leaf, close, tail): `head + open*n + leaf + close*n + tail`.
type Shape = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

const SHAPES: [Shape; 9] = [
    ("paren", "select ", "(", "1", ")", " as z;"),
    ("bracket", "select ", "[", "1", "]", " as z;"),
    ("call", "select ", "abs(", "1", ")", " as z;"),
    ("unclosed", "select ", "(", "1", "", " as z;"),
    (
        "case",
        "select ",
        "case when 1=1 then ",
        "1",
        " end",
        " as z;",
    ),
    (
        "else",
        "select ",
        "case when 1=1 then 1 else ",
        "1",
        " end",
        " as z;",
    ),
    ("not", "select 1 as z where ", "not ", "z = 1", "", ";"),
    ("filter", "select (", "z ? ", "z = 1", "", ") as y;"),
    ("type", "key x ", "list<", "int", ">", ";"),
];

fn nested(shape: Shape, depth: usize) -> String {
    let (_, head, open, leaf, close, tail) = shape;
    format!(
        "{head}{}{leaf}{}{tail}",
        open.repeat(depth),
        close.repeat(depth)
    )
}

fn is_nesting_error(text: &str) -> bool {
    match parse_bounded(text) {
        Err(e) => {
            matches!(&e.variant, ErrorVariant::CustomError { message } if message.starts_with(NESTING_LIMIT_MESSAGE))
        }
        Ok(_) => false,
    }
}

#[test]
fn shallow_nesting_parses() {
    for shape in SHAPES {
        if shape.0 == "unclosed" {
            continue;
        }
        let text = nested(shape, 20);
        assert!(parse_bounded(&text).is_ok(), "{}: {text}", shape.0);
    }
}

#[test]
fn deep_nesting_is_a_parse_error() {
    for shape in SHAPES {
        for depth in [300, 30_000] {
            let text = nested(shape, depth);
            // Unclosed input that fits the stack fails on its missing `)` first.
            let ok = if shape.0 == "unclosed" && depth == 300 {
                parse_bounded(&text).is_err()
            } else {
                is_nesting_error(&text)
            };
            assert!(ok, "{} at {depth}", shape.0);
        }
    }
}

#[test]
fn deep_nesting_fails_on_a_small_stack() {
    let text = nested(SHAPES[0], 30_000);
    let failed = std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(move || is_nesting_error(&text))
        .unwrap()
        .join()
        .unwrap();
    assert!(failed);
}

#[test]
fn parse_file_applies_the_limit() {
    let err = parse_file(&nested(SHAPES[0], 30_000)).unwrap_err();
    assert!(matches!(err, ParseError::PestError(_)));
}
