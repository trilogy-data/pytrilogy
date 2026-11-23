import json

from trilogy.scripts.agent_render import render_events


def test_result_event_renders_as_key_lines_with_compact_rows():
    stream = json.dumps(
        {
            "event": "result",
            "columns": ["name", "total"],
            "rows": [["a", 1], "<redacted 3 rows>", ["b", None]],
            "row_count": 5,
            "truncated": True,
        },
        indent=2,
    )
    assert render_events(stream).splitlines() == [
        "event: result",
        'columns: ["name","total"]',
        "rows:",
        '  ["a",1]',
        "  <redacted 3 rows>",
        '  ["b",null]',
        "row_count: 5",
        "truncated: true",
    ]


def test_events_separated_by_blank_line_and_multiline_strings_block():
    stream = (
        json.dumps({"event": "write", "path": "q.preql"})
        + "\n"
        + json.dumps({"event": "error", "message": "bad\n  ^---\nhere"})
    )
    assert render_events(stream) == (
        "event: write\npath: q.preql\n\n"
        "event: error\nmessage: |\n  bad\n    ^---\n  here"
    )


def test_nested_lists_of_dicts_and_long_scalar_lists():
    long_items = [f"concept_{i} int; # a description long enough" for i in range(4)]
    stream = json.dumps(
        {
            "namespaces": {
                "": [
                    {"keys": long_items[:1]},
                    {"grain": "<k>", "properties": long_items},
                ]
            },
            "empty": [],
            "blank": "",
        }
    )
    assert render_events(stream).splitlines() == [
        "namespaces:",
        '  "":',
        '    - keys: ["concept_0 int; # a description long enough"]',
        "    - grain: <k>",
        "      properties:",
        *[f"        - {item}" for item in long_items],
        "empty: []",
        'blank: ""',
    ]


def test_non_json_passes_through():
    assert render_events("plain text\nno json") == "plain text\nno json"
    assert render_events("") == ""
    assert render_events('{"a": 1} trailing') == '{"a": 1} trailing'
