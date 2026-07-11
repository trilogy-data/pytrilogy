'use strict';
/* Viewer page logic. Baked into viewer.html at build time (see viewer/cli.py),
   so the page works as a static file; when served (viewer/server.py) it goes
   live: eval + run pickers, polling, replay, archive, all-evals summary. */

// ---------- state ----------
let RUNS = JSON.parse(document.getElementById('data').textContent);
let SUITES = [];             // [{key,label,runs:[...]}] from suites.json
let currentSuite = null;     // suite key (null until suites.json answers)
let currentRun = null;       // results-dir name within the suite
let view = 'run';            // 'run' | 'summary'
let summaryData = null;      // last summary.json payload
let selectedName = RUNS.length ? RUNS[0].name : null;
let lastPayload = null;
let expanded = new Set();    // keys of tool-result blocks the user expanded (current run)
let compareOpen = false;     // side-by-side canonical vs agent query panel (per run)
let compareView = 'src';     // 'src' (preql/sql source) or 'sql' (rendered SQL)
let failOnly = false;        // sidebar filter: hide passing runs
let served = false;          // true once suites.json answers — replay/summary need the server
let replay = {running:false, suite:null, run:null, qid:null, log:[], result:null, error:null};
let replayTimer = null;
let archiveState = {busy:false, msg:'', ok:null};   // "archive this run to history db"

const esc = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
function badge(s){ const k=['pass','exhausted','error','fail'].includes(s)?s:'other'; return `<span class="badge ${k}">${esc(s||'?')}</span>`; }
// Sidebar result color — mirrors the dashboard PNG (STATUS_COLORS_ALT): green=pass,
// orange=fail/missed, purple=timed out (exhausted/crashed share the purple family).
function statusColor(s){
  return ({pass:'#81c784', fail:'#ffb74d', error:'#e57373', missing:'#cfcfcf',
           timeout:'#ba68c8', exhausted:'#b39ddb', crashed:'#9575cd'})[s] || '#8b93a7';
}

// ---------- syntax highlighting ----------
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

// ---------- canonical vs agent compare panel ----------
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

const fmtTok = n => n < 1000 ? String(n) : (n/1000).toFixed(n < 10000 ? 1 : 0) + 'k';
// Context cost of a tool result, from the prompt growth it caused. When one turn
// made several calls the split is proportional to output size, so it's an estimate.
function tokBadge(ev){
  if(ev.tokens == null) return '';
  const cls = ev.tokens >= 50000 ? ' huge' : ev.tokens >= 10000 ? ' big' : '';
  const tip = ev.exact ? `${ev.tokens.toLocaleString()} tokens added to context`
    : `~${ev.tokens.toLocaleString()} tokens (estimated: turn made multiple tool calls)`;
  return `<span class="tok${cls}" title="${esc(tip)}">${ev.exact?'':'~'}+${fmtTok(ev.tokens)} tok</span>`;
}

// ---------- replay ----------
const qlabel = q => 'q' + String(q).padStart(2,'0');
// The replay bar re-renders from `replay` on every poll, so a run re-render
// (live data refresh) never wipes an in-flight status.
function replayBarHtml(run){
  if(!served || !run.replayable) return '';
  // Scope status to this suite+dir+query: a job elsewhere is just "busy".
  const mine = replay.qid === run.qid && replay.run === currentRun && replay.suite === currentSuite;
  const label = (replay.running && mine) ? 'Replaying…' : 'Replay ' + qlabel(run.qid);
  const dis = replay.running ? ' disabled' : '';
  let status = '';
  if(replay.running && mine){
    status = `<span class="rpmsg">${esc(replay.log[replay.log.length-1] || 'starting…')}</span>`;
  } else if(replay.running){
    status = `<span class="rpmsg">busy: ${replay.mode==='all' ? 'full rerun' : esc(qlabel(replay.qid))}</span>`;
  } else if(mine && replay.error){
    status = `<span class="rpmsg err">${esc(replay.error)}</span>`;
  } else if(mine && replay.result){
    const r = replay.result;
    status = `<span class="rpmsg ok">${esc(r.prev_status||'absent')} → ${esc(r.status)}`
           + ` · ${r.iterations} iters · ${r.duration_seconds}s`
           + ` · pass ${r.pass_count}/${r.num_queries}</span>`;
  }
  return `<button class="cmpbtn" onclick="startReplay(${run.qid})"${dis}>${esc(label)}</button>${status}`;
}
function updateReplayBar(){
  const el = document.getElementById('rpbar'), run = RUNS.find(r=>r.name===selectedName);
  if(el && run) el.innerHTML = replayBarHtml(run);
}
// Replay state drives both the bar and the sidebar pulse; the 4s data poll is
// too coarse to start/stop the pulse on its own.
function updateReplayUi(){ updateReplayBar(); renderSide(); }
async function startReplay(qid){
  if(!confirm(`Replay ${qlabel(qid)} in ${currentRun}?\n\n`
    + `Re-seeds the semantic model, then re-runs the agent against the current `
    + `prompt and engine.\n\nOVERWRITES this query's trajectory, candidate query `
    + `and report entry. The current trajectory is not recoverable.`)) return;
  replay = {running:true, suite:currentSuite, run:currentRun, qid, log:['submitting…'], result:null, error:null};
  updateReplayUi();
  try{
    const r = await fetch('replay', {method:'POST', headers:{'Content-Type':'application/json'},
                                     body: JSON.stringify({suite: currentSuite, run: currentRun, qid})});
    const j = await r.json();
    replay = r.ok ? j : {running:false, suite:currentSuite, run:currentRun, qid, log:[], result:null, error: j.error || ('HTTP '+r.status)};
  }catch(e){ replay = {running:false, suite:currentSuite, run:currentRun, qid, log:[], result:null, error:String(e)}; }
  updateReplayUi();
  if(replay.running && !replayTimer) replayTimer = setInterval(pollReplay, 1500);
}
async function startReplayAll(){
  if(!confirm(`Rerun ALL queries in ${currentRun}?\n\n`
    + `Copies this run to a fresh sibling dir and re-runs every query there against `
    + `the current prompts, model and engine. The original run is left untouched.\n\n`
    + `This is slow — one full agent run per query.`)) return;
  replay = {running:true, mode:'all', suite:currentSuite, run:currentRun, qid:null,
            progress:{done:0,total:0,qid:null}, log:['forking…'], result:null,
            error:null, new_run:null};
  updateReplayUi();
  try{
    const r = await fetch('replay_all', {method:'POST', headers:{'Content-Type':'application/json'},
                                         body: JSON.stringify({suite: currentSuite, run: currentRun})});
    const j = await r.json();
    replay = r.ok ? j : {running:false, mode:'all', suite:currentSuite, run:currentRun, log:[], result:null,
                         error: j.error || ('HTTP '+r.status)};
  }catch(e){ replay = {running:false, mode:'all', suite:currentSuite, run:currentRun, log:[], result:null, error:String(e)}; }
  updateReplayUi();
  if(replay.running && !replayTimer) replayTimer = setInterval(pollReplay, 1500);
}
async function cancelReplay(){
  try{ await fetch('replay_cancel', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'}); }
  catch(e){ /* the poll will reflect the stop once it lands */ }
}
async function pollReplay(){
  try{
    const r = await fetch('replay_status.json', {cache:'no-store'});
    if(!r.ok) return;
    const j = await r.json();
    const finished = replay.running && !j.running;
    // A full rerun forks a new dir; follow it live (within its suite) so the
    // fork populates on screen.
    const switchTo = (j.new_run && j.suite === currentSuite && j.new_run !== currentRun) ? j.new_run : null;
    replay = j;
    if(switchTo){
      currentRun = switchTo; selectedName = null; expanded = new Set(); compareOpen = false;
      loadSuites();
    }
    updateReplayUi();
    if(finished || switchTo){
      if(finished){ clearInterval(replayTimer); replayTimer = null; }
      lastPayload = null;   // the run's logs + report just changed under us
      loadData(true);       // must win over any poll still in flight from mid-replay
    }
  }catch(e){ clearInterval(replayTimer); replayTimer = null; }
}
// A page reload mid-replay should re-attach to the running job, not lose it.
async function initReplay(){
  try{
    const r = await fetch('replay_status.json', {cache:'no-store'});
    if(!r.ok) return;
    replay = await r.json();
    updateReplayUi();
    if(replay.running && !replayTimer) replayTimer = setInterval(pollReplay, 1500);
  }catch(e){ /* static file — no replay */ }
}

// ---------- main pane: one trajectory ----------
function renderRun(run, keepScroll){
  const m = run.metrics||{}, meta = run.meta||{};
  const main = document.getElementById('main');
  const prevScroll = keepScroll ? main.scrollTop : 0;
  const rows = (m.ref_rows!=null||m.cand_rows!=null) ? `rows ${m.cand_rows??'?'}/${m.ref_rows??'?'} (cand/ref) · ` : '';
  let h = `<div class="meta">${badge(m.status)} &nbsp; <b>${esc(run.name)}</b> &nbsp;`
        + `${rows}iters ${m.iterations??'?'} · prompt_tok ${(m.prompt_tokens||0).toLocaleString()} · `
        + `${meta.provider||''} ${meta.model||''} ${m.duration_seconds?('· '+m.duration_seconds+'s'):''}</div>`;
  if(m.detail) h += `<div class="meta" style="color:var(--err)">${esc(m.detail)}</div>`;
  if(served && run.replayable) h += `<div class="cmpbar" id="rpbar">${replayBarHtml(run)}</div>`;
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
         + tokBadge(ev)
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

// ---------- sidebar ----------
function markActive(){
  document.querySelectorAll('.run').forEach(n=>
    n.classList.toggle('active', view==='run' && RUNS[+n.dataset.i].name===selectedName));
  const sb = document.getElementById('sumbtn');
  if(sb) sb.classList.toggle('on', view==='summary');
}
// The row whose query is being replayed right now — its metrics are stale until
// the job lands, so we pulse it instead of showing them.
function isReplaying(run){
  if(!replay.running || replay.suite !== currentSuite || replay.run !== currentRun) return false;
  const qid = replay.mode==='all' ? (replay.progress||{}).qid : replay.qid;
  return qid != null && qid === run.qid;
}
const PULSE_MS = 1400;
// Re-rendering the row restarts its animation. Offsetting the delay by where the
// wall clock sits in the cycle keeps the pulse continuous across re-renders.
function pulseDelay(){ return `animation-delay:${-(performance.now() % PULSE_MS)}ms`; }
const isPass = run => (run.metrics||{}).status === 'pass';
// Under the "failing" filter, keep the selected and replaying rows visible so
// the list doesn't yank the row you're reading out from under you.
function isVisible(run){
  return !failOnly || !isPass(run) || run.name === selectedName || isReplaying(run);
}
function renderTally(){
  const el = document.getElementById('tally');
  if(!el) return;
  const total = RUNS.length, pass = RUNS.filter(isPass).length, failing = total - pass;
  const pct = total ? Math.round(pass/total*100) : 0;
  el.innerHTML = `<div class="tally-n"><b>${pass}/${total}</b> pass <span class="pct">(${pct}%)</span></div>`
    + `<div class="tally-f">`
    + `<button class="fbtn${failOnly?'':' on'}" onclick="setFailOnly(false)">All ${total}</button>`
    + `<button class="fbtn${failOnly?' on':''}" onclick="setFailOnly(true)">Failing ${failing}</button>`
    + `</div>` + rerunAllHtml() + archiveHtml();
}
// Fork the current run into a fresh sibling dir and replay every query there, so
// the original stays a baseline. Server-only (needs the replay endpoints).
function rerunAllHtml(){
  if(!served) return '';
  const on = replay.running && replay.mode==='all';
  const dis = replay.running ? ' disabled' : '';
  let status = '', cancel = '';
  if(on){
    const p = replay.progress||{};
    const where = p.total ? `${qlabel(p.qid)} · ${p.done}/${p.total}`
                          : (replay.log[replay.log.length-1] || 'forking…');
    status = `<div class="rerun-st">${esc(where)}</div>`;
    cancel = `<button class="fbtn cancel" onclick="cancelReplay()">Stop after this query</button>`;
  } else if(replay.mode==='all' && replay.error){
    status = `<div class="rerun-st err">${esc(replay.error)}</div>`;
  } else if(replay.mode==='all' && replay.result){
    const r = replay.result;
    status = `<div class="rerun-st ok">reran ${r.count}/${r.total} · pass ${r.pass_count}/${r.num_queries}</div>`;
  }
  const label = on ? 'Rerunning…' : 'Rerun all → new run';
  return `<div class="rerun-bar"><button class="fbtn rerun" onclick="startReplayAll()"${dis}>`
       + `${esc(label)}</button>${status}${cancel}</div>`;
}
// Archive this run dir's summary stats into the longitudinal history db.
// Non-destructive (files stay); the CLI cleanup script archives-then-deletes.
function archiveHtml(){
  if(!served) return '';
  const dis = archiveState.busy ? ' disabled' : '';
  const label = archiveState.busy ? 'Archiving…' : 'Archive run → history db';
  let status = '';
  if(archiveState.msg){
    const cls = archiveState.ok ? ' ok' : (archiveState.ok===false ? ' err' : '');
    status = `<div class="rerun-st${cls}">${esc(archiveState.msg)}</div>`;
  }
  return `<div class="rerun-bar"><button class="fbtn rerun" onclick="archiveRun()"${dis}>`
       + `${esc(label)}</button>${status}</div>`;
}
async function archiveRun(){
  if(!currentRun) return;
  archiveState = {busy:true, msg:'archiving '+currentRun+'…', ok:null};
  renderTally();
  try{
    const r = await fetch('archive', {method:'POST', headers:{'Content-Type':'application/json'},
                                      body: JSON.stringify({suite: currentSuite, run: currentRun})});
    const j = await r.json();
    archiveState = r.ok
      ? {busy:false, ok:true, msg:`archived ${j.count} questions → ${j.db}`}
      : {busy:false, ok:false, msg: j.error || ('HTTP '+r.status)};
  }catch(e){ archiveState = {busy:false, ok:false, msg:String(e)}; }
  renderTally();
}
function setFailOnly(v){ failOnly = v; renderSide(); }
function renderSide(){
  renderTally();
  const el = document.getElementById('runs');
  const rows = RUNS.map((r,i)=>[r,i]).filter(([r])=>isVisible(r));
  if(!rows.length){ el.innerHTML = `<div class="empty">No failing runs.</div>`; return; }
  el.innerHTML = rows.map(([r,i])=>{
    const m=r.metrics||{};
    const busy = isReplaying(r);
    const c = busy ? 'var(--accent)' : statusColor(m.status);
    const tok = m.prompt_tokens||0;
    const over = (!busy && tok > 500000) ? `<span class="badge over">500k+</span>` : '';
    const tail = busy ? `<span class="badge replaying">replaying</span>` : `${badge(m.status)}${over}`;
    const sub = busy ? 'running agent…' : `${m.iterations??'?'} iters · ${(tok/1e6).toFixed(2)}M tok`;
    const delay = busy ? pulseDelay() : '';
    return `<div class="run${busy?' replaying':''}" data-i="${i}" style="${delay}"><div class="nm">`
         + `<span class="sdot${busy?' pulse':''}" style="background:${c};${delay}"></span>`
         + `<span style="color:${c}">${esc(r.name)}</span> ${tail}</div>`
         + `<div class="sub">${esc(sub)}</div></div>`;
  }).join('');
  el.querySelectorAll('.run').forEach(node=>{
    node.onclick = ()=>{ view = 'run'; selectedName = RUNS[+node.dataset.i].name; expanded = new Set();
      compareOpen = false; markActive(); renderRun(RUNS[+node.dataset.i], false); };
  });
  markActive();
}

// ---------- all-evals summary ----------
function showSummary(){
  view = 'summary';
  markActive();
  renderSummary();
  loadSummary();
}
async function loadSummary(){
  try{
    const r = await fetch('summary.json', {cache:'no-store'});
    if(!r.ok) return;
    summaryData = await r.json();
    if(view==='summary') renderSummary();
  }catch(e){ /* static file — no summary */ }
}
function openRun(suite, run){
  view = 'run';
  currentSuite = suite; currentRun = run;
  selectedName = null; lastPayload = null; expanded = new Set(); compareOpen = false;
  archiveState = {busy:false, msg:'', ok:null};
  syncPickers();
  markActive();
  loadData(true);
}
const fmtTs = ts => ts && ts.length>=8 ? `${ts.slice(0,4)}-${ts.slice(4,6)}-${ts.slice(6,8)}` : (ts||'');
const pctOf = r => r.total ? Math.round(r.passed/r.total*100) : 0;
function renderSummary(){
  const main = document.getElementById('main');
  if(!summaryData){ main.innerHTML = `<div class="meta">loading summary…</div>`; return; }
  // rows arrive newest-first; group suite -> variant -> [rows]
  const bySuite = new Map();
  for(const r of summaryData.rows){
    if(!bySuite.has(r.suite)) bySuite.set(r.suite, {label: r.suite_label || r.suite, variants: new Map()});
    const s = bySuite.get(r.suite);
    if(!s.variants.has(r.variant)) s.variants.set(r.variant, []);
    s.variants.get(r.variant).push(r);
  }
  const chip = (r, latest) => {
    const cls = 'chip' + (r.live ? ' live' : '') + (latest ? ' latest' : '');
    const tip = `${r.run} · ${fmtTs(r.ts)} · ${r.passed}/${r.total}` + (r.live ? '' : ' · archived');
    const data = r.live ? ` data-suite="${esc(r.suite)}" data-run="${esc(r.run)}"` : '';
    return `<span class="${cls}" title="${esc(tip)}"${data}>${pctOf(r)}%</span>`;
  };
  let h = `<h1 class="sum-title">Latest performance across evals</h1>`
        + `<div class="sum-note">full benchmark runs only · greyed chips are archived (raw logs deleted) · click a live run to open it</div>`;
  for(const [, s] of bySuite){
    h += `<h2 class="sum-suite">${esc(s.label)}</h2><table class="sum">`
       + `<tr><th>variant</th><th>latest run</th><th>score</th><th>trend (new → old)</th><th>model</th><th>when</th></tr>`;
    for(const [variant, list] of [...s.variants].sort((a,b)=>a[0].localeCompare(b[0]))){
      const latest = list[0];
      const name = latest.live
        ? `<span class="sum-runlink" data-suite="${esc(latest.suite)}" data-run="${esc(latest.run)}">${esc(latest.run)}</span>`
        : `${esc(latest.run)} <span class="sum-muted">(archived)</span>`;
      const chips = list.slice(0,8).map((r,i)=>chip(r, i===0)).join('');
      h += `<tr><td>${esc(variant)}</td><td>${name}</td>`
         + `<td class="sum-score"><b>${latest.passed}/${latest.total}</b> <span class="sum-muted">(${pctOf(latest)}%)</span></td>`
         + `<td>${chips}</td><td class="sum-muted">${esc(latest.model||'')}</td>`
         + `<td class="sum-muted">${fmtTs(latest.ts)}</td></tr>`;
    }
    h += `</table>`;
  }
  if(!bySuite.size) h += `<div class="meta">no runs found (live or archived)</div>`;
  main.innerHTML = h;
  main.querySelectorAll('[data-run]').forEach(n=>{
    n.onclick = ()=>openRun(n.dataset.suite, n.dataset.run);
  });
}

// ---------- data loading ----------
// Swap in fresh data without losing the user's place: keep the selected run and
// (for a background poll) its scroll position.
function applyData(runs, keepScroll){
  RUNS = runs;
  if(!RUNS.some(r=>r.name===selectedName)) selectedName = RUNS.length ? RUNS[0].name : null;
  renderSide();
  if(view !== 'run') return;   // summary open — sidebar refreshed, main pane untouched
  const sel = RUNS.find(r=>r.name===selectedName);
  if(sel) renderRun(sel, keepScroll);
}
function flashLive(){
  const el = document.getElementById('live');
  if(!el) return;
  el.textContent = '● live'; el.style.color = 'var(--ok)'; el.style.opacity = '1';
  setTimeout(()=>{ el.style.opacity = '.5'; }, 500);
}
function dataUrl(){
  const p = [];
  if(currentSuite) p.push('suite=' + encodeURIComponent(currentSuite));
  if(currentRun) p.push('run=' + encodeURIComponent(currentRun));
  return 'data.json' + (p.length ? '?' + p.join('&') : '');
}
// data.json takes tens of seconds on a cold cache (it transpiles every query),
// so polls overlap and can resolve out of order. An older snapshot must never
// overwrite a newer one: a mid-replay snapshot carries the already-truncated
// agent log but the not-yet-rewritten report.json, i.e. new iters + stale status.
let dataInFlight = 0, dataSeq = 0, appliedSeq = 0;
async function loadData(force){
  if(dataInFlight && !force) return;   // don't stack slow polls
  const seq = ++dataSeq;
  dataInFlight++;
  try{
    const r = await fetch(dataUrl(), {cache:'no-store'});
    if(!r.ok) return;
    const txt = await r.text();
    if(seq <= appliedSeq) return;   // a newer snapshot already landed
    appliedSeq = seq;
    if(txt === lastPayload) return;   // unchanged — don't disrupt the view
    lastPayload = txt;
    flashLive();
    applyData(JSON.parse(txt), true);
  }catch(e){ /* opened as a static file or server gone — keep the baked snapshot */
  }finally{ dataInFlight--; }
}
// ---------- eval + run pickers ----------
function suiteRuns(key){ const s = SUITES.find(s=>s.key===key); return s ? s.runs : []; }
function syncPickers(){
  const ss = document.getElementById('suitesel');
  ss.innerHTML = SUITES.map(s =>
    `<option value="${esc(s.key)}"${s.key===currentSuite?' selected':''}>${esc(s.label)}</option>`).join('');
  const rs = document.getElementById('runsel');
  rs.innerHTML = suiteRuns(currentSuite).map(n =>
    `<option value="${esc(n)}"${n===currentRun?' selected':''}>${esc(n)}</option>`).join('');
}
async function loadSuites(){
  try{
    const r = await fetch('suites.json', {cache:'no-store'});
    if(!r.ok) return;
    const j = await r.json();
    served = true;
    SUITES = j.suites;
    if(currentSuite === null){ currentSuite = j.current.suite; currentRun = j.current.run; }
    syncPickers();
  }catch(e){ /* static file — hide the pickers */
    const bar = document.getElementById('runbar'); if(bar) bar.style.display='none'; }
}
function onSuiteChange(){
  const key = document.getElementById('suitesel').value;
  openRun(key, suiteRuns(key)[0] || null);
}
function onRunChange(){
  openRun(currentSuite, document.getElementById('runsel').value);
}

// ---------- boot ----------
applyData(RUNS, false);
loadSuites().then(()=>loadData(true)).then(initReplay);
setInterval(()=>loadData(), 4000);
setInterval(()=>{ loadSuites(); if(view==='summary') loadSummary(); }, 10000);
