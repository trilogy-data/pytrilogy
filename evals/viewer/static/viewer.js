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
let replays = {running:false, jobs:[]};  // /replay_status.json snapshot; jobs run in parallel
let replayTimer = null;
let rrConc = 2;                 // rerun-all parallelism (UI selector)
let localErrors = {};           // rejected replay POSTs, keyed suite|run|qid
let followedForks = new Set();  // rerun-all job ids whose fork we've already jumped to
let archiveState = {busy:false, msg:'', ok:null};   // "archive this run to history db"
let loading = null;          // {run} while a user-initiated run swap waits on data.json

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
const jobKey = (s,r,q) => `${s}|${r}|${q}`;
const runningJobs = () => replays.jobs.filter(j=>j.running);
// Latest job for one query of the current suite+run: running first, else newest.
function jobFor(qid){
  const match = j => j.mode==='one' && j.suite===currentSuite && j.run===currentRun && j.qid===qid;
  return runningJobs().find(match) || [...replays.jobs].reverse().find(match) || null;
}
// The rerun-all job for the tally bar: the running one (at most one exists),
// else the newest finished one that produced or targeted the run on screen.
function allJob(){
  const running = runningJobs().find(j=>j.mode==='all');
  if(running) return running;
  return [...replays.jobs].reverse().find(j => j.mode==='all' && j.suite===currentSuite
    && (j.run===currentRun || j.new_run===currentRun)) || null;
}
// Drop optimistic placeholders (id null) after a rejected POST.
function dropPlaceholders(){
  const jobs = replays.jobs.filter(j=>j.id!=null);
  replays = {running: jobs.some(j=>j.running), jobs};
}
// The replay bar re-renders from `replays` on every poll, so a run re-render
// (live data refresh) never wipes an in-flight status. Replays run in parallel:
// only this query's own job (or a full rerun on this run) disables its button.
function replayBarHtml(run){
  if(!served || !run.replayable) return '';
  const job = jobFor(run.qid);
  const all = runningJobs().find(j=>j.mode==='all');
  const mine = !!(job && job.running);
  const blocked = mine || !!(all && all.suite===currentSuite
    && (all.run===currentRun || all.new_run===currentRun));
  const label = mine ? 'Replaying…' : 'Replay ' + qlabel(run.qid);
  const localErr = localErrors[jobKey(currentSuite, currentRun, run.qid)];
  let status = '';
  if(mine){
    status = `<span class="rpmsg">${esc(job.log[job.log.length-1] || 'starting…')}</span>`;
  } else if(localErr){
    status = `<span class="rpmsg err">${esc(localErr)}</span>`;
  } else if(job && job.error){
    status = `<span class="rpmsg err">${esc(job.error)}</span>`;
  } else if(job && job.result){
    const r = job.result;
    status = `<span class="rpmsg ok">${esc(r.prev_status||'absent')} → ${esc(r.status)}`
           + ` · ${r.iterations} iters · ${r.duration_seconds}s`
           + ` · pass ${r.pass_count}/${r.num_queries}</span>`;
  }
  return `<button class="cmpbtn" onclick="startReplay(${run.qid})"${blocked?' disabled':''}>${esc(label)}</button>${status}`;
}
function updateReplayBar(){
  const el = document.getElementById('rpbar'), run = RUNS.find(r=>r.name===selectedName);
  if(el && run) el.innerHTML = replayBarHtml(run);
}
// Replay state drives both the bar and the sidebar pulse; the 4s data poll is
// too coarse to start/stop the pulse on its own.
function updateReplayUi(){ updateReplayBar(); renderSide(); }
function ensureReplayTimer(){
  if(replays.running && !replayTimer) replayTimer = setInterval(pollReplay, 1500);
}
async function startReplay(qid){
  if(!confirm(`Replay ${qlabel(qid)} in ${currentRun}?\n\n`
    + `Re-seeds the semantic model, then re-runs the agent against the current `
    + `prompt and engine.\n\nOVERWRITES this query's trajectory, candidate query `
    + `and report entry. The current trajectory is not recoverable.`)) return;
  const key = jobKey(currentSuite, currentRun, qid);
  delete localErrors[key];
  // Optimistic placeholder until the server's snapshot answers.
  replays = {running:true, jobs:[...replays.jobs,
    {id:null, running:true, mode:'one', suite:currentSuite, run:currentRun, qid,
     progress:null, new_run:null, log:['submitting…'], result:null, error:null}]};
  updateReplayUi();
  try{
    const r = await fetch('replay', {method:'POST', headers:{'Content-Type':'application/json'},
                                     body: JSON.stringify({suite: currentSuite, run: currentRun, qid})});
    const j = await r.json();
    if(r.ok) replays = j;
    else{ dropPlaceholders(); localErrors[key] = j.error || ('HTTP '+r.status); }
  }catch(e){ dropPlaceholders(); localErrors[key] = String(e); }
  updateReplayUi();
  ensureReplayTimer();
}
async function startReplayAll(){
  if(!confirm(`Rerun ALL queries in ${currentRun}?\n\n`
    + `Copies this run to a fresh sibling dir and re-runs every query there against `
    + `the current prompts, model and engine (${rrConc} at a time). The original run `
    + `is left untouched.\n\nThis is slow — one full agent run per query.`)) return;
  const key = jobKey(currentSuite, currentRun, 'all');
  delete localErrors[key];
  replays = {running:true, jobs:[...replays.jobs,
    {id:null, running:true, mode:'all', suite:currentSuite, run:currentRun, qid:null,
     progress:{done:0,total:0,qid:null,active:[]}, new_run:null, log:['forking…'],
     result:null, error:null}]};
  updateReplayUi();
  try{
    const r = await fetch('replay_all', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({suite: currentSuite, run: currentRun, concurrency: rrConc})});
    const j = await r.json();
    if(r.ok) replays = j;
    else{ dropPlaceholders(); localErrors[key] = j.error || ('HTTP '+r.status); }
  }catch(e){ dropPlaceholders(); localErrors[key] = String(e); }
  updateReplayUi();
  ensureReplayTimer();
}
async function cancelReplay(id){
  const body = id!=null ? JSON.stringify({id}) : '{}';
  try{ await fetch('replay_cancel', {method:'POST', headers:{'Content-Type':'application/json'}, body}); }
  catch(e){ /* the poll will reflect the stop once it lands */ }
}
async function pollReplay(){
  try{
    const r = await fetch('replay_status.json', {cache:'no-store'});
    if(!r.ok) return;
    const j = await r.json();
    const nowRunning = new Set(j.jobs.filter(x=>x.running).map(x=>x.id));
    const finished = runningJobs().some(x=>x.id!=null && !nowRunning.has(x.id));
    replays = j;
    // A full rerun forks a new dir; follow it live (within its suite) so the
    // fork populates on screen — once per job, so later navigation sticks.
    const fork = j.jobs.find(x => x.mode==='all' && x.new_run
                                  && x.suite===currentSuite && !followedForks.has(x.id));
    let switched = false;
    if(fork){
      followedForks.add(fork.id);
      if(fork.new_run !== currentRun){
        currentRun = fork.new_run; selectedName = null; expanded = new Set(); compareOpen = false;
        switched = true;
        loadSuites();
      }
    }
    updateReplayUi();
    if(!j.running && replayTimer){ clearInterval(replayTimer); replayTimer = null; }
    if(finished || switched){
      lastPayload = null;   // the run's logs + report just changed under us
      if(switched){ loading = {run: currentRun}; renderLoading(); }
      loadData(true);       // must win over any poll still in flight from mid-replay
    }
  }catch(e){ clearInterval(replayTimer); replayTimer = null; }
}
// A page reload mid-replay should re-attach to the running jobs, not lose them.
async function initReplay(){
  try{
    const r = await fetch('replay_status.json', {cache:'no-store'});
    if(!r.ok) return;
    replays = await r.json();
    // Forks from before this page load are history, not something to jump to.
    for(const j of replays.jobs) if(!j.running && j.new_run != null) followedForks.add(j.id);
    updateReplayUi();
    ensureReplayTimer();
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
// Rows whose queries are being replayed right now — their metrics are stale
// until the jobs land, so we pulse them instead of showing them.
function isReplaying(run){
  for(const j of runningJobs()){
    if(j.suite !== currentSuite || (j.run !== currentRun && j.new_run !== currentRun)) continue;
    if(j.mode === 'all'){
      const p = j.progress || {};
      const act = (p.active && p.active.length) ? p.active : (p.qid != null ? [p.qid] : []);
      if(act.includes(run.qid)) return true;
    } else if(j.qid === run.qid) return true;
  }
  return false;
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
  const job = allJob();
  const on = !!(job && job.running);
  const localErr = localErrors[jobKey(currentSuite, currentRun, 'all')];
  let status = '', cancel = '';
  if(on){
    const p = job.progress||{};
    const act = (p.active||[]).map(qlabel).join(' ');
    const where = p.total ? `${act || (p.qid!=null ? qlabel(p.qid) : '')} · ${p.done}/${p.total}`
                          : (job.log[job.log.length-1] || 'forking…');
    status = `<div class="rerun-st">${esc(where)}</div>`;
    cancel = `<button class="fbtn cancel" onclick="cancelReplay(${job.id})">Finish in-flight, then stop</button>`;
  } else if(localErr){
    status = `<div class="rerun-st err">${esc(localErr)}</div>`;
  } else if(job && job.error){
    status = `<div class="rerun-st err">${esc(job.error)}</div>`;
  } else if(job && job.result){
    const r = job.result;
    const errs = r.errors ? ` · ${r.errors} errored` : '';
    status = `<div class="rerun-st ok">reran ${r.count}/${r.total}${errs} · pass ${r.pass_count}/${r.num_queries}</div>`;
  }
  const label = on ? 'Rerunning…' : 'Rerun all → new run';
  const conc = on ? '' : ` <select class="cmpsel" title="parallel agents" onchange="rrConc=+this.value">`
    + [1,2,4].map(n=>`<option value="${n}"${n===rrConc?' selected':''}>${n}×</option>`).join('')
    + `</select>`;
  return `<div class="rerun-bar"><button class="fbtn rerun" onclick="startReplayAll()"${on?' disabled':''}>`
       + `${esc(label)}</button>${conc}${status}${cancel}</div>`;
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
// Swapping runs is a server round-trip: data.json re-collects the run, and the
// first open of a run transpiles every query to SQL (tens of seconds). Show
// that instead of leaving the old run frozen on screen.
function renderLoading(){
  document.getElementById('tally').innerHTML = '';
  document.getElementById('runs').innerHTML =
    `<div class="empty">loading ${esc(loading.run||'run')}…</div>`;
  if(view==='run'){
    document.getElementById('main').innerHTML =
      `<div class="meta loadmsg"><span class="spin"></span>loading ${esc(loading.run||'run')} — `
      + `parsing agent logs and rendering SQL (first open of a run can take a while)…</div>`;
  }
}
function renderSide(){
  if(loading){ renderLoading(); return; }
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
  loading = {run};
  syncPickers();
  markActive();
  renderLoading();
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
  loading = null;
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
// A snapshot fetched for a different suite/run than the one now on screen is
// dropped too — otherwise a stale poll flashes the old run mid-swap.
let dataInFlight = 0, dataSeq = 0, appliedSeq = 0;
async function loadData(force){
  if(dataInFlight && !force) return;   // don't stack slow polls
  const seq = ++dataSeq, url = dataUrl();
  dataInFlight++;
  try{
    const r = await fetch(url, {cache:'no-store'});
    if(!r.ok) return;
    const txt = await r.text();
    if(seq <= appliedSeq || url !== dataUrl()) return;
    appliedSeq = seq;
    if(txt === lastPayload) return;   // unchanged — don't disrupt the view
    lastPayload = txt;
    flashLive();
    applyData(JSON.parse(txt), true);
  }catch(e){ /* opened as a static file or server gone — keep the baked snapshot */
  }finally{
    dataInFlight--;
    // The swap this spinner belongs to settled without delivering data
    // (error / server gone) — stop spinning and say so.
    if(loading && url === dataUrl() && appliedSeq < seq){
      loading = null;
      if(view==='run') document.getElementById('main').innerHTML =
        `<div class="meta">failed to load ${esc(currentRun||'run')} — is the server still running?</div>`;
      renderSide();
    }
  }
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
