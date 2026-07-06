#!/usr/bin/env python
"""Bundle agent trajectory logs into one self-contained HTML viewer.

    python trajectory_viewer.py <results_dir> [--serve PORT]

Reads every ``agent_log.*.jsonl`` in the directory (plus ``repeat_report.json``
for per-run status/metrics when present) and writes ``<results_dir>/viewer.html``
— a static page with a run picker and a conversation timeline. No external
assets. With ``--serve`` it also starts a local http server on that port.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_RUN_RE = re.compile(r"\.r(\d+)\.jsonl$")
_QID_RE = re.compile(r"\.q(\d+)\.jsonl$")
_QID_ANY = re.compile(r"\.q(\d+)\b")  # query id in either eval or repeat log names
# Which candidate/canonical extension each eval leg authors.
_CATEGORY_EXT = {
    "enriched": "preql",
    "ingest": "preql",
    "sql_bare": "sql",
    "sql_schema": "sql",
}


def _call_label(name: str, args: list[str]) -> str:
    if name == "trilogy" and args:
        head = [a for a in args if not a.startswith("--")][:3]
        return "trilogy " + " ".join(head)
    return name


def _extract_content(args: list[str]) -> str | None:
    """The written payload of a `trilogy file write` — passed as `-c`/`--content`."""
    for flag in ("--content", "-c"):
        if flag in args:
            i = args.index(flag)
            if i + 1 < len(args):
                return args[i + 1]
    return None


def _result_ok(output: str) -> bool:
    low = output.lower()
    if "exit_code: 0" in low:
        return True
    if any(
        k in low for k in ("error", "traceback", "exit_code: 1", '"event": "error"')
    ):
        return False
    return True


def _reviewer_input(e: dict) -> str:
    """The exact input the reviewer saw: system prompt + agent-only transcript
    (older logs predate these fields and render an empty input)."""
    parts = []
    if e.get("system_prompt"):
        parts.append("SYSTEM PROMPT:\n" + e["system_prompt"])
    if e.get("transcript"):
        parts.append("AGENT'S RECENT MESSAGES:\n" + e["transcript"])
    return "\n\n".join(parts)


def _read_events(path: Path) -> list[dict]:
    """Tolerant JSONL read — skip blank/half-written lines so a log that's still
    being appended to (live run) parses cleanly up to the last complete record."""
    events: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def parse_log(path: Path) -> dict:
    events = _read_events(path)
    meta: dict = {"task": "", "model": "", "provider": ""}
    timeline: list[dict] = []
    prompt = completion = total = iterations = tool_calls = 0
    for e in events:
        t = e.get("type")
        if t == "session_start":
            meta["task"] = e.get("command", "")
            meta["model"] = e.get("model", "")
            meta["provider"] = e.get("provider", "")
        elif t == "llm_response":
            calls = []
            for c in e.get("tool_calls") or []:
                args = (c.get("arguments") or {}).get("args") or []
                calls.append(
                    {
                        "label": _call_label(c.get("name", ""), args),
                        "args": args,
                        "content": _extract_content(args),
                    }
                )
            u = e.get("usage") or {}
            prompt += u.get("prompt_tokens", 0)
            completion += u.get("completion_tokens", 0)
            total += u.get("total_tokens", 0)
            iterations += 1
            tool_calls += len(calls)
            timeline.append(
                {
                    "role": "assistant",
                    "text": e.get("text") or "",
                    "calls": calls,
                    "usage": u,
                }
            )
        elif t == "tool_result":
            out = e.get("result")
            if not isinstance(out, str):
                out = json.dumps(out, indent=1)
            timeline.append(
                {
                    "role": "tool",
                    "name": e.get("name", ""),
                    "ok": _result_ok(out),
                    "output": out,
                }
            )
        elif t == "reviewer_verdict":
            done = bool(e.get("is_done"))
            timeline.append(
                {
                    "role": "reviewer",
                    "verdict": "DONE" if done else "NOT_DONE",
                    "ok": done,
                    "note": e.get("note") or "",
                    "kickback": e.get("kickback_count", 0),
                    "input": _reviewer_input(e),
                }
            )
        elif t == "reviewer_bypassed":
            timeline.append(
                {
                    "role": "reviewer",
                    "verdict": "BYPASSED",
                    "ok": True,
                    "note": e.get("reason") or "force=true",
                    "kickback": 0,
                }
            )
    derived = {
        "iterations": iterations,
        "tool_calls": tool_calls,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    }
    return {"meta": meta, "timeline": timeline, "derived": derived}


def _load_eval_report(results_dir: Path) -> dict[int, dict]:
    """run_eval's report.json: merge per-query status + duration, keyed by query id."""
    rp = results_dir / "report.json"
    if not rp.exists():
        return {}
    data = json.loads(rp.read_text(encoding="utf-8"))
    by_id: dict[int, dict] = {q["id"]: dict(q) for q in data.get("queries", [])}
    for p in data.get("per_query", []):
        by_id.setdefault(p["id"], {}).update(
            {k: p[k] for k in ("duration_seconds", "timed_out") if k in p}
        )
    return by_id


def _find_ref_dir(results_dir: Path) -> Path | None:
    """Locate the hand-authored canonical query dir by walking up to the repo root."""
    for anc in results_dir.parents:
        cand = anc / "tests" / "modeling" / "tpc_ds_duckdb"
        if cand.is_dir():
            return cand
    return None


def _read_query(base: Path | None, qid: int, ext: str) -> dict | None:
    if base is None:
        return None
    for name in (f"query{qid:02d}.{ext}", f"query{qid}.{ext}"):
        p = base / name
        if p.exists():
            return {
                "name": name,
                "lang": ext,
                "src": p.read_text(encoding="utf-8"),
                "_path": p,
                "_base": base,
            }
    return None


# --- preql → SQL transpilation (best-effort; the viewer stays usable without it) ---
_TRANSPILE_ENGINE: object | None = None
_ENGINE_FAILED = False
_SQL_CACHE: dict[tuple, tuple] = {}  # (path, mtime, working_path) -> (sql, is_error)
_PARAMS: dict[int, dict] | None = None


def _transpile_engine():
    """A DB-less in-memory DuckDB executor reused across renders — generate_sql is
    pure transpilation, so we never open the (large) workspace database."""
    global _TRANSPILE_ENGINE, _ENGINE_FAILED
    if _TRANSPILE_ENGINE is None and not _ENGINE_FAILED:
        try:
            from trilogy import Dialects
            from trilogy.core.models.environment import Environment
            from trilogy.dialect.config import DuckDBConfig

            _TRANSPILE_ENGINE = Dialects.DUCK_DB.default_executor(
                environment=Environment(), conf=DuckDBConfig()
            )
        except Exception:
            _ENGINE_FAILED = True
    return _TRANSPILE_ENGINE


def _load_params() -> dict[int, dict]:
    global _PARAMS
    if _PARAMS is None:
        _PARAMS = {}
        pf = Path(__file__).parent / "query_prompts.json"
        if pf.exists():
            data = json.loads(pf.read_text(encoding="utf-8"))
            qs = data.get("queries", []) if isinstance(data, dict) else data
            _PARAMS = {q["id"]: q["params"] for q in qs if q.get("params")}
    return _PARAMS


def _render_sql(text: str, working_path: Path, params: dict | None) -> tuple[str, bool]:
    engine = _transpile_engine()
    if engine is None:
        return "", True
    from trilogy.core.models.environment import Environment

    try:
        engine.environment = Environment(working_path=working_path)
        if params:
            engine.environment.set_parameters(
                **{name: spec.get("value") for name, spec in params.items()}
            )
        statements = engine.generate_sql(text)
        return (statements[-1] if statements else "-- (no statement)"), False
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}", True


def _augment_sql(q: dict | None, params: dict | None) -> dict | None:
    """Attach rendered SQL (cached by file mtime) and strip internal path fields."""
    if q is None:
        return None
    base, path = q.pop("_base"), q.pop("_path")
    if q["lang"] != "preql":
        q["sql"], q["sqlError"] = q["src"], False  # SQL legs: candidate is already SQL
        return q
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    key = (str(path), mtime, str(base))
    if key not in _SQL_CACHE:
        _SQL_CACHE[key] = _render_sql(q["src"], base, params)
    q["sql"], q["sqlError"] = _SQL_CACHE[key]
    return q


def _run_category(results_dir: Path) -> str | None:
    """The eval leg (enriched/ingest/sql_bare/sql_schema) — picks the query language."""
    rp = results_dir / "repeat_report.json"
    if rp.exists():
        return json.loads(rp.read_text(encoding="utf-8")).get("meta", {}).get("mode")
    ep = results_dir / "report.json"
    if ep.exists():
        return (
            json.loads(ep.read_text(encoding="utf-8")).get("meta", {}).get("category")
        )
    return None


def collect(results_dir: Path) -> list[dict]:
    report = {}
    repeat_qid = None
    rp = results_dir / "repeat_report.json"
    if rp.exists():
        data = json.loads(rp.read_text(encoding="utf-8"))
        report = {r["rep"]: r for r in data.get("runs", [])}
        repeat_qid = data.get("meta", {}).get("query_id")
    eval_report = _load_eval_report(results_dir)
    ext = _CATEGORY_EXT.get(_run_category(results_dir) or "", "preql")
    ref_dir = _find_ref_dir(results_dir)
    workspace = results_dir / "workspace"
    runs: list[dict] = []
    for path in sorted(results_dir.glob("agent_log.*.jsonl")):
        m = _RUN_RE.search(path.name)
        rep = int(m.group(1)) if m else len(runs)
        qm = _QID_RE.search(path.name)
        reported = report.get(rep, {})
        if not reported and qm:
            reported = eval_report.get(int(qm.group(1)), {})
        parsed = parse_log(path)
        # Derived per-run metrics fill gaps; authoritative report values win.
        metrics = {**parsed["derived"], **reported}
        qany = _QID_ANY.search(path.name)
        qid = int(qany.group(1)) if qany else repeat_qid
        queries = {}
        if qid is not None:
            # Candidate at workspace root (single-leg runs), else the rep's worker dir.
            candidate = _read_query(workspace, qid, ext) or _read_query(
                workspace / f"_worker_{rep}", qid, ext
            )
            params = _load_params().get(qid)
            queries = {
                "candidate": _augment_sql(candidate, params),
                "canonical": _augment_sql(_read_query(ref_dir, qid, ext), params),
            }
        runs.append(
            {
                "name": path.name.replace("agent_log.", "").replace(".jsonl", ""),
                "rep": rep,
                "meta": parsed["meta"],
                "timeline": parsed["timeline"],
                "metrics": metrics,
                "queries": queries,
            }
        )
    return runs


_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent trajectories — __TITLE__</title>
<style>
  :root { --bg:#0f1117; --panel:#171a23; --panel2:#1e222e; --border:#2a2f3d;
          --txt:#d7dbe6; --muted:#8b93a7; --accent:#5b9dff; --ok:#3fb950; --err:#f85149;
          --asst:#1b2436; --tool:#161b22; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--txt); }
  #wrap { display:flex; height:100vh; }
  #side { width:260px; flex:0 0 260px; border-right:1px solid var(--border); overflow-y:auto; background:var(--panel); }
  #side h1 { font-size:13px; padding:14px 14px 6px; margin:0; color:var(--muted); letter-spacing:.04em; text-transform:uppercase; }
  .run { padding:10px 14px; border-bottom:1px solid var(--border); cursor:pointer; }
  .run:hover { background:var(--panel2); }
  .run.active { background:var(--panel2); border-left:3px solid var(--accent); padding-left:11px; }
  .run .nm { font-weight:600; }
  .run .sub { font-size:12px; color:var(--muted); margin-top:2px; }
  .badge { display:inline-block; padding:1px 7px; border-radius:10px; font-size:11px; font-weight:600; }
  .badge.pass { background:rgba(63,185,80,.18); color:var(--ok); }
  .badge.exhausted,.badge.error,.badge.fail { background:rgba(248,81,73,.18); color:var(--err); }
  .badge.other { background:rgba(139,147,167,.18); color:var(--muted); }
  #main { flex:1; overflow-y:auto; padding:22px 28px 80px; }
  #task { background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:12px 16px; margin-bottom:20px; white-space:pre-wrap; color:var(--muted); font-size:13px; max-height:160px; overflow:auto; }
  .turn { margin:0 0 14px; }
  .turn .who { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); margin-bottom:4px; }
  .bubble { border:1px solid var(--border); border-radius:8px; padding:10px 14px; }
  .assistant .bubble { background:var(--asst); }
  .tool .bubble { background:var(--tool); }
  .reviewer .bubble { background:#2a2410; border-color:#5c4a12; }
  .reviewer .vok { color:var(--ok); }
  .reviewer .verr { color:var(--err); }
  .text { white-space:pre-wrap; }
  .call { margin-top:8px; font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:12.5px; }
  .call .cmd { color:var(--accent); font-weight:600; }
  pre { margin:8px 0 0; padding:10px 12px; background:#0c0e14; border:1px solid var(--border); border-radius:6px; overflow:auto; font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:12.5px; white-space:pre; }
  .tool .head { display:flex; align-items:center; gap:8px; cursor:pointer; }
  .dot { width:8px; height:8px; border-radius:50%; flex:0 0 8px; }
  .dot.ok { background:var(--ok); } .dot.err { background:var(--err); }
  .tool .name { font-family:ui-monospace,monospace; font-size:12.5px; color:var(--muted); }
  .out { display:none; } .out.show { display:block; }
  .usage { font-size:11px; color:var(--muted); margin-top:6px; }
  .meta { color:var(--muted); font-size:13px; margin-bottom:14px; }
  .chev { color:var(--muted); font-size:11px; margin-left:auto; }
  #live { font-size:10px; margin-left:6px; opacity:.5; transition:opacity .4s; }
  #runbar { padding:12px 14px 4px; }
  #runbar label { font-size:11px; color:var(--muted); letter-spacing:.04em; text-transform:uppercase; }
  #runsel { width:100%; margin-top:5px; background:var(--panel2); color:var(--txt);
            border:1px solid var(--border); border-radius:6px; padding:6px 8px; font-size:12px; }
  .cmpbar { display:flex; align-items:center; gap:10px; margin-bottom:16px; }
  .cmpbtn { background:var(--panel2); color:var(--txt); border:1px solid var(--border);
            border-radius:6px; padding:5px 12px; font-size:12px; cursor:pointer; }
  .cmpbtn:hover { border-color:var(--accent); color:var(--accent); }
  .cmpsel { background:var(--panel2); color:var(--txt); border:1px solid var(--border);
            border-radius:6px; padding:5px 8px; font-size:12px; cursor:pointer; }
  .cmp { display:flex; gap:12px; margin-bottom:20px; }
  .cmp-col { flex:1; min-width:0; }
  .cmp-h { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); margin-bottom:4px; }
  .cmp-col pre { margin:0; max-height:70vh; }
  .code.err { color:var(--err); white-space:pre-wrap; }
  .hl-kw { color:#ff7ab6; font-weight:600; }
  .hl-st { color:#7ee787; }
  .hl-id { color:#79c0ff; }
  .hl-cm { color:#8b93a7; font-style:italic; }
  .hl-nu { color:#f0a45d; }
  .hl-op { color:#ff7ab6; }
</style>
</head>
<body>
<div id="wrap">
  <div id="side">
    <div id="runbar"><label>Run<span id="live"></span></label><select id="runsel" onchange="onRunChange()"></select></div>
    <div id="runs"></div>
  </div>
  <div id="main"></div>
</div>
<script id="data" type="application/json">__DATA__</script>
<script>
let RUNS = JSON.parse(document.getElementById('data').textContent);
let selectedName = RUNS.length ? RUNS[0].name : null;
let lastPayload = null;
let expanded = new Set();   // keys of tool-result blocks the user expanded (current run)
let currentRun = null;   // which results dir we're viewing (null until runs.json loads)
let compareOpen = false;   // side-by-side canonical vs agent query panel (per run)
let compareView = 'src';   // 'src' (preql/sql source) or 'sql' (rendered SQL)
const esc = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
function badge(s){ const k=['pass','exhausted','error','fail'].includes(s)?s:'other'; return `<span class="badge ${k}">${esc(s||'?')}</span>`; }

const SQL_KW = new Set(('select from where group by order having limit offset join left right '
  + 'full inner outer cross on using as and or not in is null case when then else end with union '
  + 'all distinct asc desc between like ilike over partition rows range unbounded preceding '
  + 'following current row cast exists interval coalesce nulls first last').split(' '));
const PREQL_KW = new Set(('import as auto def rowset merge property key datasource grain address '
  + 'where select order by having limit filter into with union all and or not is null case when '
  + 'then else end between like over partition asc desc true false raw const type persist show '
  + 'rollup by where').split(' '));
function sp(c,t){ return `<span class="hl-${c}">${esc(t)}</span>`; }
function hl(code, lang){
  const kw = lang==='preql' ? PREQL_KW : SQL_KW;
  const re = /(--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^']|'')*')|("(?:[^"]|"")*")|(<-|->|::|\?)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)/g;
  let out='', last=0, m;
  while((m=re.exec(code))){
    out += esc(code.slice(last, m.index));
    last = re.lastIndex;
    if(m[1]) out += sp('cm', m[1]);
    else if(m[2]) out += sp('st', m[2]);
    else if(m[3]) out += sp('id', m[3]);
    else if(m[4]) out += sp('op', m[4]);
    else if(m[5]) out += sp('nu', m[5]);
    else out += SQL_KW.has(m[6].toLowerCase()) || kw.has(m[6].toLowerCase()) ? sp('kw', m[6]) : esc(m[6]);
  }
  return out + esc(code.slice(last));
}
function hasPreql(q){ return ['candidate','canonical'].some(k=>q[k] && q[k].lang==='preql'); }
function compareHtml(run){
  const q = run.queries||{};
  if(!q.candidate && !q.canonical) return '';
  const sql = compareView==='sql';
  const body = o => {
    if(!o) return `<pre class="code">(not found)</pre>`;
    const txt = sql ? o.sql : o.src, lang = sql ? 'sql' : o.lang;
    if(sql && o.sqlError) return `<pre class="code err">${esc(txt)}</pre>`;
    return `<pre class="code">${hl(txt, lang)}</pre>`;
  };
  const label = o => !o ? 'n/a' : o.name + (sql && o.lang==='preql' ? ' → SQL' : '');
  const col = (who,o) => `<div class="cmp-col"><div class="cmp-h">${esc(who)} · ${esc(label(o))}</div>${body(o)}</div>`;
  return `<div class="cmp">`+col('canonical', q.canonical)+col('agent', q.candidate)+`</div>`;
}

function renderRun(run, keepScroll){
  const m = run.metrics||{}, meta = run.meta||{};
  const main = document.getElementById('main');
  const prevScroll = keepScroll ? main.scrollTop : 0;
  const rows = (m.ref_rows!=null||m.cand_rows!=null) ? `rows ${m.cand_rows??'?'}/${m.ref_rows??'?'} (cand/ref) · ` : '';
  let h = `<div class="meta">${badge(m.status)} &nbsp; <b>${esc(run.name)}</b> &nbsp;`
        + `${rows}iters ${m.iterations??'?'} · prompt_tok ${(m.prompt_tokens||0).toLocaleString()} · `
        + `${meta.provider||''} ${meta.model||''} ${m.duration_seconds?('· '+m.duration_seconds+'s'):''}</div>`;
  if(m.detail) h += `<div class="meta" style="color:var(--err)">${esc(m.detail)}</div>`;
  h += `<div id="task">${esc(meta.task)}</div>`;
  const q = run.queries||{};
  if(q.candidate || q.canonical){
    h += `<div class="cmpbar"><button class="cmpbtn" onclick="toggleCompare()">`
       + `${compareOpen?'Hide':'Compare'} canonical vs agent query</button>`;
    if(compareOpen && hasPreql(q)){
      h += `<select class="cmpsel" onchange="setCompareView(this.value)">`
         + `<option value="src"${compareView==='src'?' selected':''}>Trilogy (preql)</option>`
         + `<option value="sql"${compareView==='sql'?' selected':''}>Rendered SQL</option></select>`;
    }
    if(compareOpen) h += `<button class="cmpbtn" onclick="copyBoth(this)">Copy both</button>`;
    h += `</div>`;
    if(compareOpen) h += compareHtml(run);
  }
  for(let i=0;i<run.timeline.length;i++){
    const ev = run.timeline[i];
    if(ev.role==='assistant'){
      h += `<div class="turn assistant"><div class="who">assistant</div><div class="bubble">`;
      if(ev.text) h += `<div class="text">${esc(ev.text)}</div>`;
      for(const c of ev.calls){
        h += `<div class="call"><span class="cmd">$ ${esc(c.label)}</span>`;
        if(c.content!=null) h += `<pre>${esc(c.content)}</pre>`;
        h += `</div>`;
      }
      const u = ev.usage||{};
      if(u.total_tokens) h += `<div class="usage">prompt ${u.prompt_tokens} · completion ${u.completion_tokens} · total ${u.total_tokens}</div>`;
      h += `</div></div>`;
    } else if(ev.role==='reviewer'){
      const kb = ev.kickback ? ` (kickback ${ev.kickback})` : '';
      h += `<div class="turn reviewer"><div class="who">reviewer · <span class="${ev.ok?'vok':'verr'}">${esc(ev.verdict)}</span>${esc(kb)}</div><div class="bubble">`;
      if(ev.note) h += `<div class="text">${esc(ev.note)}</div>`;
      if(ev.input){
        const k = 'r'+i, show = expanded.has(k) ? ' show' : '';
        h += `<div class="head" data-key="${k}" onclick="toggleOut(this)">`
           + `<span class="name">reviewer input (what we sent)</span>`
           + `<span class="chev">▼ click to toggle</span></div>`
           + `<pre class="out${show}">${esc(ev.input)}</pre>`;
      }
      h += `</div></div>`;
    } else {
      // Stable per-position key so an expanded block stays expanded across the
      // auto-refresh re-render (timeline only grows; existing indices are fixed).
      const k = 't'+i, show = expanded.has(k) ? ' show' : '';
      h += `<div class="turn tool"><div class="bubble">`
         + `<div class="head" data-key="${k}" onclick="toggleOut(this)">`
         + `<span class="dot ${ev.ok?'ok':'err'}"></span><span class="name">${esc(ev.name)} result</span>`
         + `<span class="chev">▼ click to toggle</span></div>`
         + `<pre class="out${show}">${esc(ev.output)}</pre></div></div>`;
    }
  }
  main.innerHTML = h;
  main.scrollTop = prevScroll;
}

function toggleOut(head){
  const pre = head.nextElementSibling, k = head.dataset.key;
  if(pre.classList.toggle('show')) expanded.add(k); else expanded.delete(k);
}
function toggleCompare(){
  compareOpen = !compareOpen;
  const sel = RUNS.find(r=>r.name===selectedName);
  if(sel) renderRun(sel, true);
}
function setCompareView(v){
  compareView = v;
  const sel = RUNS.find(r=>r.name===selectedName);
  if(sel) renderRun(sel, true);
}
function writeClip(text){
  if(navigator.clipboard && navigator.clipboard.writeText) return navigator.clipboard.writeText(text);
  return new Promise((res,rej)=>{
    const ta=document.createElement('textarea'); ta.value=text;
    ta.style.position='fixed'; ta.style.left='-9999px'; document.body.appendChild(ta); ta.select();
    try{ document.execCommand('copy'); res(); }catch(e){ rej(e); } finally{ document.body.removeChild(ta); }
  });
}
function copyBoth(btn){
  const run = RUNS.find(r=>r.name===selectedName);
  if(!run) return;
  const q = run.queries||{}, sql = compareView==='sql';
  const suffix = o => sql && o.lang==='preql' ? ' → SQL' : '';
  const block = (who,o) => {
    if(!o) return `-- ===== ${who} (n/a) =====\n(not found)`;
    const err = sql && o.sqlError ? '-- (render error)\n' : '';
    return `-- ===== ${who} (${o.name}${suffix(o)}) =====\n` + err + (sql ? o.sql : o.src);
  };
  const text = block('CANONICAL', q.canonical) + '\n\n' + block('AGENT', q.candidate) + '\n';
  const old = btn.textContent;
  writeClip(text).then(()=>{ btn.textContent='Copied ✓'; }).catch(()=>{ btn.textContent='Copy failed'; });
  setTimeout(()=>{ btn.textContent=old; }, 1200);
}

function markActive(){
  document.querySelectorAll('.run').forEach(n=>
    n.classList.toggle('active', RUNS[+n.dataset.i].name===selectedName));
}
function renderSide(){
  const el = document.getElementById('runs');
  el.innerHTML = RUNS.map((r,i)=>{
    const m=r.metrics||{};
    return `<div class="run" data-i="${i}"><div class="nm">${esc(r.name)} ${badge(m.status)}</div>`
         + `<div class="sub">${m.iterations??'?'} iters · ${((m.prompt_tokens||0)/1e6).toFixed(2)}M tok</div></div>`;
  }).join('');
  el.querySelectorAll('.run').forEach(node=>{
    node.onclick = ()=>{ selectedName = RUNS[+node.dataset.i].name; expanded = new Set();
      compareOpen = false; markActive(); renderRun(RUNS[+node.dataset.i], false); };
  });
  markActive();
}
// Swap in fresh data without losing the user's place: keep the selected run and
// (for a background poll) its scroll position.
function applyData(runs, keepScroll){
  RUNS = runs;
  if(!RUNS.some(r=>r.name===selectedName)) selectedName = RUNS.length ? RUNS[0].name : null;
  renderSide();
  const sel = RUNS.find(r=>r.name===selectedName);
  if(sel) renderRun(sel, keepScroll);
}
function flashLive(){
  const el = document.getElementById('live');
  if(!el) return;
  el.textContent = '● live'; el.style.color = 'var(--ok)'; el.style.opacity = '1';
  setTimeout(()=>{ el.style.opacity = '.5'; }, 500);
}
async function loadData(){
  try{
    const q = currentRun ? ('?run=' + encodeURIComponent(currentRun)) : '';
    const r = await fetch('data.json' + q, {cache:'no-store'});
    if(!r.ok) return;
    const txt = await r.text();
    if(txt === lastPayload) return;   // unchanged — don't disrupt the view
    lastPayload = txt;
    flashLive();
    applyData(JSON.parse(txt), true);
  }catch(e){ /* opened as a static file or server gone — keep the baked snapshot */ }
}
// Populate the run-directory dropdown from sibling results dirs.
async function loadRuns(){
  try{
    const r = await fetch('runs.json', {cache:'no-store'});
    if(!r.ok) return;
    const j = await r.json();
    if(currentRun === null) currentRun = j.current;
    const sel = document.getElementById('runsel');
    sel.innerHTML = j.runs.map(n =>
      `<option value="${esc(n)}"${n===currentRun?' selected':''}>${esc(n)}</option>`).join('');
    sel.value = currentRun;
  }catch(e){ /* static file — hide the picker */
    const bar = document.getElementById('runbar'); if(bar) bar.style.display='none'; }
}
function onRunChange(){
  currentRun = document.getElementById('runsel').value;
  selectedName = null; lastPayload = null; expanded = new Set(); compareOpen = false;   // reset for the new run
  loadData();
}
applyData(RUNS, false);
loadRuns().then(loadData);
setInterval(loadData, 4000);
setInterval(loadRuns, 10000);
</script>
</body>
</html>
"""


def build_html(results_dir: Path) -> Path:
    runs = collect(results_dir)
    if not runs:
        raise SystemExit(f"no agent_log.*.jsonl found in {results_dir}")
    data = json.dumps(runs).replace("</", "<\\/")
    html = _HTML.replace("__TITLE__", results_dir.name).replace("__DATA__", data)
    out = results_dir / "viewer.html"
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out}  ({len(runs)} runs)")
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("results_dir", type=Path)
    p.add_argument("--serve", type=int, default=None, help="serve the dir on this port")
    args = p.parse_args()
    build_html(args.results_dir)
    if args.serve is not None:
        import functools
        import http.server
        import socketserver
        import urllib.parse

        results_dir = args.results_dir
        root = results_dir.parent  # sibling run dirs live alongside us

        def list_run_dirs() -> list[str]:
            dirs = [
                c
                for c in root.iterdir()
                if c.is_dir() and any(c.glob("agent_log.*.jsonl"))
            ]
            dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return [p.name for p in dirs]

        class _Handler(http.server.SimpleHTTPRequestHandler):
            def _json(self, obj) -> None:
                payload = json.dumps(obj).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == "/runs.json":
                    self._json({"runs": list_run_dirs(), "current": results_dir.name})
                    return
                if parsed.path == "/data.json":
                    # Re-read the chosen run's logs per request so a live (or
                    # just-finished) run streams in — the page polls this.
                    qs = urllib.parse.parse_qs(parsed.query)
                    name = (qs.get("run") or [results_dir.name])[0]
                    target = root / name if name in list_run_dirs() else results_dir
                    self._json(collect(target))
                    return
                super().do_GET()

        handler = functools.partial(_Handler, directory=str(results_dir))
        with socketserver.TCPServer(("127.0.0.1", args.serve), handler) as httpd:
            print(
                f"serving http://127.0.0.1:{args.serve}/viewer.html  (ctrl-c to stop)"
            )
            httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
