'use strict';
/* Viewer page logic. Baked into viewer.html at build time (see viewer/cli.py),
   so a run dir carries a standalone copy of itself; when served (viewer/server.py)
   the same page goes live and gains the eval summary, the Launch screen and the
   cross-run grid.

   Three pages behind the ribbon: Evals (how every benchmark is doing), Launch
   (start runs) and Debug (the question-by-run grid, drilling into one
   trajectory). Fetching is lazy by design - the grid reads only report.json,
   a question's log is read when you open it, and its SQL render only when you
   ask to compare - so nothing waits on work you didn't ask for. */

// ---------- state ----------
const PRELOAD = JSON.parse(document.getElementById('data').textContent || '{}');
let served = false;          // true once suites.json answers
let page = 'debug';          // 'evals' | 'launch' | 'debug'
let SUITES = [];             // [{key,label,runs:[...]}] from suites.json
let curSuite = PRELOAD.suite || null;
let curRun = PRELOAD.run || null;
let curKey = null;           // selected question key ("q05", "q96.r3")

// debug page
let dbgView = 'grid';        // 'grid' | 'drill'
let gridLimit = 25, gridFilter = '', gridProblems = false, gridQ = null, gridOnDisk = false;
// Correlations live inside a category (an enriched regression says nothing
// about sql_bare), so the grid groups by it and scores each group separately.
let gridGroup = true, gridGroupLimit = 10, gridMinQ = 0;
let collapsed = new Set();   // collapsed category groups
let lastGroups = [];         // what the last render drew, for hover lookups
let qFailOnly = false;       // drilldown question list filter
let expanded = new Set();    // tool-result blocks the user expanded
let compareOpen = false, compareView = 'src';
let hoverText = '';

// caches (see fetchJSON) - the page never re-reads what it already has
const MATRIX = {};           // suite -> matrix payload
const INDEX = {};            // suite|run -> run index
const TRAJ = {};             // suite|run|key -> trajectory
const QUERIES = {};          // suite|run|key -> {candidate, canonical}
let matrixProgress = null;   // {done,total} while a cold grid build reads runs
let matrixError = '';

// jobs
let replays = {running:false, jobs:[]};
let replayTimer = null, rrConc = 2;
let localErrors = {}, followedForks = new Set();
let archiveState = {busy:false, msg:'', ok:null};

// launch screen
let launchOpts = null, launchSuite = null, launchForms = {};
let launchJobs = {running:false, queued:0, jobs:[]};
let launchPreview = '', launchError = '', launchBusy = false, previewTimer = null;
let logOpen = new Set(), logSeen = new Set();

// ---------- helpers ----------
const esc = s => (s == null ? '' : String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const rkey = (s, r) => `${s}|${r}`;
const tkey = (s, r, k) => `${s}|${r}|${k}`;
const qlabel = q => 'q' + String(q).padStart(2,'0');
const fmtTok = n => !n ? '0' : n < 1000 ? String(n) : n < 1e6 ? (n/1000).toFixed(0)+'k' : (n/1e6).toFixed(2)+'M';
const fmtDur = s => s == null ? '' : s < 60 ? `${Math.round(s)}s` : s < 3600 ? `${Math.floor(s/60)}m ${Math.round(s%60)}s` : `${Math.floor(s/3600)}h ${Math.round(s%3600/60)}m`;
const fmtTs = ts => ts && ts.length >= 13 ? `${ts.slice(4,6)}-${ts.slice(6,8)} ${ts.slice(9,11)}:${ts.slice(11,13)}` : (ts||'');
function badge(s){ const k = ['pass','exhausted','error','fail'].includes(s) ? s : 'other'; return `<span class="badge ${k}">${esc(s||'?')}</span>`; }
// Status colors mirror the dashboard PNG: green=pass, orange=fail, red=error,
// purple family=out of time, blue=in flight.
const STATUS_COLOR = {pass:'#3fb950', fail:'#ffb74d', error:'#f85149', missing:'#5c6370',
                      timeout:'#ba68c8', exhausted:'#b39ddb', crashed:'#9575cd',
                      running:'#5b9dff', unscored:'#8b93a7', partial:'#e6c07b'};
const statusColor = s => STATUS_COLOR[s] || '#8b93a7';

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

// ---------- fetching ----------
// One in-flight request per URL (a poll landing on top of a load reuses it),
// and a thin top bar whenever anything is outstanding, so a slow read looks
// like progress rather than a frozen page.
const INFLIGHT = new Map();
let busy = 0;
function showBusy(){
  const el = document.getElementById('busy');
  if(el) el.className = busy > 0 ? 'on' : '';
}
async function fetchJSON(url){
  if(INFLIGHT.has(url)) return INFLIGHT.get(url);
  const p = (async () => {
    busy++; showBusy();
    try{
      const r = await fetch(url, {cache:'no-store'});
      const j = await r.json();
      if(!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
      return j;
    } finally { busy--; showBusy(); INFLIGHT.delete(url); }
  })();
  INFLIGHT.set(url, p);
  return p;
}
const runQS = (suite, run) => `suite=${encodeURIComponent(suite)}&run=${encodeURIComponent(run)}`;

async function loadMatrix(suite){
  try{
    const j = await fetchJSON(`matrix.json?suite=${encodeURIComponent(suite)}`);
    if(j.ready){
      MATRIX[suite] = j; matrixProgress = null; matrixError = '';
    } else if(j.error){
      matrixError = j.error; matrixProgress = null;
    } else {
      // Cold build of a big results dir: keep the bar moving, ask again soon.
      matrixProgress = j.progress || {done:0, total:0};
      setTimeout(() => loadMatrix(suite), 250);
    }
  }catch(e){ matrixError = String(e.message || e); matrixProgress = null; }
  if(page==='debug' && dbgView==='grid') renderGrid();
}
async function loadIndex(suite, run, force){
  const key = rkey(suite, run);
  if(INDEX[key] && !force) return INDEX[key];
  INDEX[key] = await fetchJSON(`run_index.json?${runQS(suite, run)}`);
  return INDEX[key];
}
async function loadTrajectory(suite, run, key){
  const k = tkey(suite, run, key);
  if(TRAJ[k]) return TRAJ[k];
  TRAJ[k] = await fetchJSON(`trajectory.json?${runQS(suite, run)}&q=${encodeURIComponent(key)}`);
  return TRAJ[k];
}
async function loadQueries(suite, run, key, category){
  const k = tkey(suite, run, key);
  if(QUERIES[k]) return QUERIES[k];
  QUERIES[k] = await fetchJSON(`queries.json?${runQS(suite, run)}&q=${encodeURIComponent(key)}`
    + (category ? `&category=${encodeURIComponent(category)}` : ''));
  return QUERIES[k];
}
// A replay rewrites one question's log, candidate query and report row.
function invalidateRun(suite, run, key){
  delete MATRIX[suite];
  delete INDEX[rkey(suite, run)];
  if(key){ delete TRAJ[tkey(suite, run, key)]; delete QUERIES[tkey(suite, run, key)]; }
  else for(const k of Object.keys(TRAJ)) if(k.startsWith(rkey(suite, run) + '|')) delete TRAJ[k];
}

// ---------- routing ----------
function go(next){
  page = next;
  for(const p of ['evals','launch','debug']){
    const b = document.getElementById('rib-' + p);
    if(b) b.classList.toggle('on', p === page);
  }
  if(page === 'evals') showSummary();
  else if(page === 'launch') showLaunch();
  else showDebug();
}
const pageEl = () => document.getElementById('page');
function spinner(text){ return `<div class="meta loadmsg"><span class="spin"></span>${esc(text)}</div>`; }
function progressBar(done, total, text){
  const pct = total ? Math.round(done/total*100) : 0;
  return `<div class="pbar"><div class="pbar-t">${esc(text)} <b>${done}/${total}</b></div>`
       + `<div class="pbar-r"><div class="pbar-f" style="width:${pct}%"></div></div></div>`;
}

// ---------- page: debug / grid ----------
function showDebug(){
  if(!served){ renderDrill(); return; }   // static file: the baked run is all there is
  if(dbgView === 'drill' && curRun) renderDrill();
  else { renderGrid(); if(curSuite && !MATRIX[curSuite]) loadMatrix(curSuite); }
}
// Everything that passes the filters, before any row cap.
function gridMatches(m){
  const needle = gridFilter.trim().toLowerCase();
  let rows = m.runs;
  if(gridOnDisk) rows = rows.filter(r => r.on_disk);
  if(gridMinQ) rows = rows.filter(r => (r.total || 0) >= gridMinQ);
  if(needle) rows = rows.filter(r => (r.name + ' ' + (r.category||'') + ' ' + (r.model||'')).toLowerCase().includes(needle));
  return rows;
}
// One group per category (or a single unnamed group when grouping is off),
// newest-active first, each capped on its own so a busy category can't crowd
// the others out of view.
function gridGroups(m){
  const rows = gridMatches(m);
  if(!gridGroup) return [{key:null, all:rows, rows: gridLimit ? rows.slice(0, gridLimit) : rows}];
  const by = new Map();
  for(const r of rows){
    const key = r.category || '?';
    if(!by.has(key)) by.set(key, []);
    by.get(key).push(r);
  }
  const groups = [...by].map(([key, all]) => ({
    key, all, rows: gridGroupLimit ? all.slice(0, gridGroupLimit) : all,
  }));
  groups.sort((a, b) => (b.all[0]?.ts || '').localeCompare(a.all[0]?.ts || '') || a.key.localeCompare(b.key));
  return groups;
}
const shownRows = groups => groups.flatMap(g => collapsed.has(g.key) ? [] : g.rows);
function gridRows(m){ return shownRows(gridGroups(m)); }
// Columns worth showing: with "problems only" on, drop questions that pass in
// every visible run - what's left is the cluster you care about.
function gridColumns(m, rows){
  const cols = m.questions.map((q, i) => i);
  if(!gridProblems) return cols;
  return cols.filter(i => rows.some(r => {
    const c = r.cells[i];
    return c && c !== '.' && c.toLowerCase() !== 'p';
  }));
}
function columnHealth(rows, i){
  let pass = 0, ran = 0;
  for(const r of rows){
    const c = (r.cells[i] || '.').toLowerCase();
    if(c === '.') continue;
    ran++; if(c === 'p') pass++;
  }
  return {pass, ran};
}
function setGrid(field, value){
  if(field === 'limit') gridLimit = value;
  else if(field === 'grouplimit') gridGroupLimit = value;
  else if(field === 'filter') gridFilter = value;
  else if(field === 'problems') gridProblems = value;
  else if(field === 'ondisk') gridOnDisk = value;
  else if(field === 'group') gridGroup = value;
  else if(field === 'minq') gridMinQ = Math.max(0, +value || 0);
  else if(field === 'suite'){ curSuite = value; gridQ = null; if(!MATRIX[value]) loadMatrix(value); }
  renderGrid();
}
function pickColumn(i){ gridQ = (gridQ === i ? null : i); renderGrid(); }
function toggleGroup(key){
  if(collapsed.has(key)) collapsed.delete(key); else collapsed.add(key);
  renderGrid();
}
function setAllGroups(open){
  collapsed = open ? new Set() : new Set(lastGroups.map(g => g.key));
  renderGrid();
}
function renderGrid(){
  if(page !== 'debug' || dbgView !== 'grid') return;
  const m = MATRIX[curSuite];
  const suiteSel = `<select class="sel" onchange="setGrid('suite', this.value)">`
    + SUITES.map(s => `<option value="${esc(s.key)}"${s.key===curSuite?' selected':''}>${esc(s.label)}</option>`).join('')
    + `</select>`;
  const most = m ? Math.max(0, ...m.runs.map(r => r.total || 0)) : 0;
  const limits = gridGroup
    ? [5, 10, 0].map(n => `<button class="fbtn${gridGroupLimit===n?' on':''}" `
        + `onclick="setGrid('grouplimit',${n})" title="runs shown per category">${n||'all'}</button>`).join('')
    : [25, 50, 0].map(n => `<button class="fbtn${gridLimit===n?' on':''}" `
        + `onclick="setGrid('limit',${n})" title="runs shown">${n||'all'}</button>`).join('');
  let h = `<div class="dbgbar">${suiteSel}`
    + `<input class="lin" type="text" placeholder="filter runs (category, model, name)" value="${esc(gridFilter)}"`
    + ` oninput="setGrid('filter', this.value)">`
    + `<span class="lnote">${gridGroup ? 'per group' : 'runs'}</span>${limits}`
    + `<span class="lnote">min questions</span>`
    + `<input class="lin num" type="number" min="0" value="${gridMinQ}" onchange="setGrid('minq', this.value)">`
    + (most ? `<button class="fbtn${gridMinQ===most?' on':''}" onclick="setGrid('minq',${most})"`
        + ` title="only full runs">full ${most}</button>` : '')
    + (gridMinQ ? `<button class="fbtn" onclick="setGrid('minq',0)">any</button>` : '')
    + `<label class="lcheck"><input type="checkbox"${gridGroup?' checked':''}`
    + ` onchange="setGrid('group', this.checked)"> group by category</label>`
    + `<label class="lcheck"><input type="checkbox"${gridProblems?' checked':''}`
    + ` onchange="setGrid('problems', this.checked)"> problem questions only</label>`
    + `<label class="lcheck"><input type="checkbox"${gridOnDisk?' checked':''}`
    + ` onchange="setGrid('ondisk', this.checked)"> on disk only</label>`
    + (gridGroup ? `<button class="fbtn" onclick="setAllGroups(false)">collapse all</button>`
        + `<button class="fbtn" onclick="setAllGroups(true)">expand all</button>` : '')
    + `<span class="grow1"></span><span class="ghover" id="ghover">${esc(hoverText)}</span></div>`;
  if(!m){
    h += matrixError ? `<div class="rerun-st err">${esc(matrixError)}</div>`
       : matrixProgress ? progressBar(matrixProgress.done, matrixProgress.total, 'reading runs')
       : spinner('loading runs…');
    pageEl().innerHTML = h;
    return;
  }
  const groups = gridGroups(m);
  lastGroups = groups;
  const rows = shownRows(groups), cols = gridColumns(m, rows);
  h += `<div class="gridwrap"><div class="grid" style="--ncol:${cols.length}">`;
  // header: question numbers, clickable to focus a column
  h += `<div class="grow ghead"><div class="glabel gcorner">${rows.length} of ${m.runs.length} runs</div>`
     + cols.map(i => `<i class="gh${gridQ===i?' on':''}" data-q="${i}">`
        + `${(m.questions[i] % 5 === 0 || cols.length <= 30) ? m.questions[i] : ''}</i>`).join('')
     + `</div>`;
  const healthStrip = (label, groupRows) =>
    `<div class="grow"><div class="glabel gcorner sub">${esc(label)}</div>`
    + cols.map(i => {
        const {pass, ran} = columnHealth(groupRows, i);
        const rate = ran ? pass/ran : 0;
        const bg = ran ? `hsl(${Math.round(120*rate)},58%,${28 + Math.round(18*rate)}%)` : 'transparent';
        return `<i class="gc" data-q="${i}" style="background:${bg}"></i>`;
      }).join('') + `</div>`;
  const runRow = r => {
    const cells = cols.map(i => {
      const c = r.cells[i] || '.';
      const st = m.legend[c.toLowerCase()] || null;
      const cls = 'gc' + (c === '.' ? ' none' : '') + (c !== c.toLowerCase() ? ' spliced' : '')
        + (gridQ === i ? ' col' : '');
      const bg = st ? statusColor(st) : '';
      return `<i class="${cls}" data-q="${i}"${bg?` style="background:${bg}"`:''}></i>`;
    }).join('');
    return `<div class="grow${r.kind==='live'?' live':''}${r.on_disk?'':' arch'}" data-run="${esc(r.name)}">`
       + `<div class="glabel" title="${esc(r.name)}${r.on_disk?'':' (archived: logs reclaimed)'}">`
       + (gridGroup ? '' : `<span class="gcat">${esc(r.category||'?')}</span>`)
       + `<span class="gts">${esc(fmtTs(r.ts))}</span>`
       + `<span class="gmodel">${esc(r.model||'')}</span>`
       + `<span class="gscore">${r.total ? r.passed + '/' + r.total : ''}</span></div>`
       + cells + `</div>`;
  };
  if(!gridGroup){
    h += healthStrip('pass rate', rows) + rows.map(runRow).join('');
  } else {
    for(const g of groups){
      const shut = collapsed.has(g.key);
      const [pass, ran] = g.all.reduce((a, r) => [a[0] + (r.passed||0), a[1] + (r.total||0)], [0, 0]);
      h += `<div class="ggroup" data-group="${esc(g.key)}">`
         + `<div class="ggh" onclick="toggleGroup('${esc(g.key)}')">`
         + `<span class="gchev">${shut ? '▸' : '▾'}</span><b>${esc(g.key)}</b>`
         + `<span class="sum-muted">${g.rows.length === g.all.length ? g.all.length + ' runs'
             : g.rows.length + ' of ' + g.all.length + ' runs'}`
         + `${ran ? ` · ${Math.round(100*pass/ran)}% pass` : ''}</span></div>`;
      if(!shut) h += healthStrip('pass rate', g.rows) + g.rows.map(runRow).join('');
      h += `</div>`;
    }
  }
  h += `</div></div>`;
  const kept = m.runs.length, live = m.runs.filter(r => r.on_disk).length;
  h += `<div class="glegend">` + Object.entries(m.legend).map(([c, st]) =>
        `<span class="lg"><i style="background:${statusColor(st)}"></i>${esc(st)}</span>`).join('')
     + `<span class="lg"><i class="spliced" style="background:${statusColor('pass')}"></i>spliced from an earlier run</span>`
     + `<span class="lg dim">click a cell for its trajectory · a run name to open the run · a question number to focus it</span>`
     + `<span class="lg dim">${live} of ${kept} runs still on disk; the rest are kept in the history db</span></div>`;
  pageEl().innerHTML = h;
  wireGrid(m, rows);
}
// One listener for the whole grid: 25 runs x 99 questions is 2,500 cells, and
// per-cell handlers (or title attributes) are what make a grid that size crawl.
function wireGrid(m, rows){
  const grid = pageEl().querySelector('.grid');
  if(!grid) return;
  const at = e => {
    const cell = e.target.closest('.gc, .gh');
    const row = e.target.closest('.grow');
    if(!row) return null;
    return {
      run: row.dataset.run || null,
      qi: cell && cell.dataset.q != null ? +cell.dataset.q : null,
      isHead: !!(cell && cell.classList.contains('gh')),
    };
  };
  grid.onclick = e => {
    const hit = at(e);
    if(!hit) return;
    if(hit.isHead || !hit.run){ if(hit.qi != null) pickColumn(hit.qi); return; }
    const q = hit.qi != null ? m.questions[hit.qi] : null;
    openRun(curSuite, hit.run, q != null ? qlabel(q) : null);
  };
  grid.onmousemove = e => {
    const hit = at(e), label = document.getElementById('ghover');
    if(!hit || !label) return;
    const q = hit.qi != null ? m.questions[hit.qi] : null;
    let text = hit.run || '';
    if(q != null){
      const row = rows.find(r => r.name === hit.run);
      if(row){
        const c = row.cells[hit.qi] || '.';
        text = `${hit.run} · ${qlabel(q)} · ${m.legend[c.toLowerCase()] || 'not run'}`
             + `${c !== c.toLowerCase() ? ' (spliced)' : ''}`;
      } else {
        // A pass-rate strip: report the group it belongs to, which is the
        // comparison that means anything.
        const key = (e.target.closest('.ggroup') || {dataset:{}}).dataset.group;
        const scope = lastGroups.find(g => g.key === key);
        const {pass, ran} = columnHealth(scope ? scope.rows : rows, hit.qi);
        text = `${qlabel(q)} · ${pass}/${ran} pass` + (key ? ` in ${key}` : ' across shown runs');
      }
    }
    if(text !== hoverText){ hoverText = text; label.textContent = text; }
  };
}

// ---------- page: debug / drilldown ----------
async function openRun(suite, run, key){
  curSuite = suite; curRun = run; curKey = key || null;
  dbgView = 'drill';
  compareOpen = false; expanded = new Set(); archiveState = {busy:false, msg:'', ok:null};
  go('debug');
  try{
    const index = await loadIndex(suite, run);
    if(!curKey){
      // Land on something worth looking at: a failure with a trajectory, else
      // anything with one, else (an archived run) the first question at all.
      const openable = index.questions.filter(q => q.has_log);
      const pool = openable.length ? openable : index.questions;
      const first = pool.find(q => q.status !== 'pass') || pool[0];
      curKey = first ? first.key : null;
    }
    renderDrill();
    if(curKey) selectQuestion(curKey);
  }catch(e){ pageEl().innerHTML = `<div class="meta">could not open ${esc(run)}: ${esc(e.message||e)}</div>`; }
}
function pickRun(name){ openRun(curSuite, name, null); }
function backToGrid(){
  dbgView = 'grid'; curKey = null;
  renderGrid();
  if(!MATRIX[curSuite]) loadMatrix(curSuite);
}
async function selectQuestion(key){
  curKey = key; compareOpen = false; expanded = new Set();
  renderDrill();
  const q = currentQuestion();
  if(!q || !q.has_log) return;   // archived or spliced: there is no log to fetch
  try{ await loadTrajectory(curSuite, curRun, key); }
  catch(e){ /* rendered as a load failure below */ }
  renderDrill();
}
function currentIndex(){ return INDEX[rkey(curSuite, curRun)] || null; }
function currentQuestion(){
  const index = currentIndex();
  return index ? index.questions.find(q => q.key === curKey) || null : null;
}
function setQFilter(v){ qFailOnly = v; renderDrill(); }
function renderDrill(){
  const index = currentIndex();
  if(!index){ pageEl().innerHTML = spinner(`loading ${curRun || 'run'}…`); return; }
  // An archived run isn't in the picker (its files are gone), so it is added
  // explicitly - otherwise the select would show someone else's name.
  const known = (SUITES.find(s => s.key === curSuite) || {}).runs || [];
  const runs = known.includes(curRun) ? known : [curRun, ...known];
  const picker = served && runs.length ? `<select class="sel" onchange="pickRun(this.value)">`
    + runs.map(n => `<option value="${esc(n)}"${n===curRun?' selected':''}>${esc(n)}</option>`).join('') + `</select>` : '';
  const s = index.summary;
  let h = `<div class="crumb">`
    + (served ? `<button class="fbtn" onclick="backToGrid()">← grid</button>` : '')
    + `<b class="crumb-run" title="${esc(index.name)}">${esc(index.name)}</b>`
    + (index.archived ? `<span class="badge other">archived</span>` : '')
    + (index.curated ? `<span class="badge other" title="results spliced in from an earlier run or replayed offline">curated</span>` : '')
    + `<span class="sum-muted">${esc(index.category||'')} · ${esc(index.model||'')}`
    + `${index.scale_factor!=null?' · sf '+index.scale_factor:''}</span>`
    + `<span class="crumb-score"><b>${s.passed}/${s.total}</b> pass · ${fmtTok(s.prompt_tokens)} tok</span>`
    + picker + `<span class="grow1"></span>` + rerunAllHtml() + archiveHtml() + `</div>`;
  if(index.archived) h += `<div class="archnote">The run's files were reclaimed`
    + `${index.archived_at ? ' (archived ' + esc(index.archived_at.slice(0,10)) + ')' : ''}. `
    + `Per-question results and the agent's final query are kept in the history db; the agent logs are not.</div>`;
  const failing = index.questions.filter(q => q.status !== 'pass').length;
  const questions = index.questions.filter(q => !qFailOnly || q.status !== 'pass');
  h += `<div class="drill">`
    + `<aside class="qlist"><div class="qfilter">`
    + `<button class="fbtn${qFailOnly?'':' on'}" onclick="setQFilter(false)">All ${index.questions.length}</button>`
    + `<button class="fbtn${qFailOnly?' on':''}" onclick="setQFilter(true)">Failing ${failing}</button>`
    + `</div><div class="qrows">`
    + questions.map(q => {
        const busyNow = isReplaying(q);
        const color = busyNow ? 'var(--accent)' : statusColor(q.status);
        const name = q.key || (q.qid != null ? qlabel(q.qid) : '?');
        const sub = busyNow ? 'running agent…'
          : `${q.iterations!=null?q.iterations+' it':'-'} · ${fmtTok(q.prompt_tokens||0)} tok`
            + (q.source && q.source !== 'this_run' ? ' · spliced' : '');
        const over = (q.prompt_tokens||0) > 500000 ? `<span class="badge over">500k+</span>` : '';
        // Dim only what this run can't show: inside an archived run nothing has
        // a log, so dimming every row would just be noise.
        const dim = !q.has_log && !index.archived ? ' nolog' : '';
        return `<div class="qrow${q.key===curKey?' active':''}${busyNow?' replaying':''}${dim}"`
          + ` data-key="${esc(q.key||'')}" title="${esc(q.detail||'')}">`
          + `<span class="sdot${busyNow?' pulse':''}" style="background:${color}"></span>`
          + `<span class="qn">${esc(name)}</span>${over}`
          + `<span class="qst" style="color:${color}">${esc(busyNow?'replaying':q.status)}</span>`
          + `<div class="qsub">${esc(sub)}</div></div>`;
      }).join('')
    + `</div></aside><section class="qview" id="qview">${questionHtml()}</section></div>`;
  pageEl().innerHTML = h;
  const list = pageEl().querySelector('.qrows');
  if(list) list.onclick = e => {
    const row = e.target.closest('.qrow');
    if(row && row.dataset.key) selectQuestion(row.dataset.key);
  };
}
function questionHtml(){
  const q = currentQuestion();
  if(!q) return `<div class="meta">pick a question.</div>`;
  if(!q.has_log) return noTrajectoryHtml(q);
  const traj = TRAJ[tkey(curSuite, curRun, curKey)];
  return traj ? trajectoryHtml(q, traj) : spinner(`loading ${curKey}…`);
}
// Archived (files reclaimed) or spliced (never ran here): the result and, for
// an archived run, the query it wrote are still worth showing.
function noTrajectoryHtml(q){
  const index = currentIndex() || {};
  const why = q.source && q.source !== 'this_run'
    ? `its result was spliced in from <b>${esc(q.source)}</b>, so it never ran here`
    : `this run's agent logs were reclaimed`;
  const rows = (q.ref_rows != null || q.cand_rows != null)
    ? `rows ${q.cand_rows??'?'}/${q.ref_rows??'?'} (cand/ref) · ` : '';
  let h = `<div class="meta">${badge(q.status)} &nbsp; <b>${esc(q.key)}</b> &nbsp; ${rows}`
        + `iters ${q.iterations ?? '?'} · prompt_tok ${(q.prompt_tokens||0).toLocaleString()}`
        + `${q.duration_seconds ? ' · ' + fmtDur(q.duration_seconds) : ''}</div>`;
  if(q.detail) h += `<div class="meta" style="color:var(--err)">${esc(q.detail)}</div>`;
  h += `<div class="meta">No trajectory: ${why}.</div>`;
  const pair = QUERIES[tkey(curSuite, curRun, curKey)];
  if(index.archived && q.has_query !== false){
    h += `<div class="cmpbar"><button class="cmpbtn" onclick="toggleCompare()">`
       + `${compareOpen?'Hide':'Compare'} canonical vs agent query</button>`;
    if(compareOpen && pair && hasPreql(pair)){
      h += `<select class="sel" onchange="setCompareView(this.value)">`
         + `<option value="src"${compareView==='src'?' selected':''}>Trilogy (preql)</option>`
         + `<option value="sql"${compareView==='sql'?' selected':''}>Rendered SQL</option></select>`;
    }
    if(compareOpen && pair) h += `<button class="cmpbtn" onclick="copyBoth(this)">Copy both</button>`;
    h += `</div>`;
    if(compareOpen) h += pair ? compareHtml(pair) : spinner('loading the archived query…');
  }
  return h;
}
function renderQuestion(keepScroll){
  const view = document.getElementById('qview');
  if(!view) return;
  const prev = keepScroll ? view.scrollTop : 0;
  view.innerHTML = questionHtml();
  view.scrollTop = prev;
}
function trajectoryHtml(q, traj){
  const meta = traj.meta || {};
  const rows = (q.ref_rows != null || q.cand_rows != null)
    ? `rows ${q.cand_rows??'?'}/${q.ref_rows??'?'} (cand/ref) · ` : '';
  let h = `<div class="meta">${badge(q.status)} &nbsp; <b>${esc(q.key)}</b> &nbsp; ${rows}`
        + `iters ${q.iterations ?? traj.derived.iterations} · `
        + `prompt_tok ${(q.prompt_tokens ?? traj.derived.prompt_tokens ?? 0).toLocaleString()} · `
        + `${esc(meta.provider||'')} ${esc(meta.model||'')}`
        + `${q.duration_seconds ? ' · ' + fmtDur(q.duration_seconds) : ''}</div>`;
  if(q.detail) h += `<div class="meta" style="color:var(--err)">${esc(q.detail)}</div>`;
  if(served && (currentIndex()||{}).replayable && q.qid != null)
    h += `<div class="cmpbar" id="rpbar">${replayBarHtml(q)}</div>`;
  h += `<div id="task">${esc(meta.task)}</div>`;
  const pair = QUERIES[tkey(curSuite, curRun, curKey)];
  h += `<div class="cmpbar"><button class="cmpbtn" onclick="toggleCompare()">`
     + `${compareOpen?'Hide':'Compare'} canonical vs agent query</button>`;
  if(compareOpen && pair && hasPreql(pair)){
    h += `<select class="sel" onchange="setCompareView(this.value)">`
       + `<option value="src"${compareView==='src'?' selected':''}>Trilogy (preql)</option>`
       + `<option value="sql"${compareView==='sql'?' selected':''}>Rendered SQL</option></select>`;
  }
  if(compareOpen && pair) h += `<button class="cmpbtn" onclick="copyBoth(this)">Copy both</button>`;
  h += `</div>`;
  if(compareOpen) h += pair ? compareHtml(pair) : spinner('rendering SQL (the first render boots the engine)…');
  for(let i=0;i<traj.timeline.length;i++){
    const ev = traj.timeline[i];
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
      // Stable per-position key so an expanded block stays expanded across a
      // re-render (the timeline only grows; existing indices are fixed).
      const k = 't'+i, show = expanded.has(k) ? ' show' : '';
      h += `<div class="turn tool"><div class="bubble">`
         + `<div class="head" data-key="${k}" onclick="toggleOut(this)">`
         + `<span class="dot ${ev.ok?'ok':'err'}"></span><span class="name">${esc(ev.name)} result</span>`
         + tokBadge(ev)
         + `<span class="chev">▼ click to toggle</span></div>`
         + `<pre class="out${show}">${esc(ev.output)}</pre></div></div>`;
    }
  }
  return h;
}
// Context cost of a tool result, from the prompt growth it caused. When one turn
// made several calls the split is proportional to output size, so it's an estimate.
function tokBadge(ev){
  if(ev.tokens == null) return '';
  const cls = ev.tokens >= 50000 ? ' huge' : ev.tokens >= 10000 ? ' big' : '';
  const tip = ev.exact ? `${ev.tokens.toLocaleString()} tokens added to context`
    : `~${ev.tokens.toLocaleString()} tokens (estimated: turn made multiple tool calls)`;
  return `<span class="tok${cls}" title="${esc(tip)}">${ev.exact?'':'~'}+${fmtTok(ev.tokens)} tok</span>`;
}
function toggleOut(head){
  const pre = head.nextElementSibling, k = head.dataset.key;
  if(pre.classList.toggle('show')) expanded.add(k); else expanded.delete(k);
}

// ---------- canonical vs agent compare ----------
function hasPreql(pair){ return ['candidate','canonical'].some(k => pair[k] && pair[k].lang === 'preql'); }
function compareHtml(pair){
  if(!pair.candidate && !pair.canonical) return `<div class="meta">no query files for this question.</div>`;
  const sql = compareView === 'sql';
  const body = o => {
    if(!o) return `<pre class="code">(not found)</pre>`;
    const txt = sql ? o.sql : o.src, lang = sql ? 'sql' : o.lang;
    if(sql && o.sqlError) return `<pre class="code err">${esc(txt)}</pre>`;
    return `<pre class="code">${hl(txt, lang)}</pre>`;
  };
  const label = o => !o ? 'n/a' : o.name + (sql && o.lang==='preql' ? ' → SQL' : '');
  const col = (who,o) => `<div class="cmp-col"><div class="cmp-h">${esc(who)} · ${esc(label(o))}</div>${body(o)}</div>`;
  return `<div class="cmp">` + col('canonical', pair.canonical) + col('agent', pair.candidate) + `</div>`;
}
async function toggleCompare(){
  compareOpen = !compareOpen;
  renderQuestion(true);
  if(!compareOpen) return;
  const index = currentIndex();
  try{ await loadQueries(curSuite, curRun, curKey, index && index.category); }
  catch(e){ QUERIES[tkey(curSuite, curRun, curKey)] = {candidate:null, canonical:null}; }
  renderQuestion(true);
}
function setCompareView(v){ compareView = v; renderQuestion(true); }
function writeClip(text){
  if(navigator.clipboard && navigator.clipboard.writeText) return navigator.clipboard.writeText(text);
  return new Promise((res,rej)=>{
    const ta=document.createElement('textarea'); ta.value=text;
    ta.style.position='fixed'; ta.style.left='-9999px'; document.body.appendChild(ta); ta.select();
    try{ document.execCommand('copy'); res(); }catch(e){ rej(e); } finally{ document.body.removeChild(ta); }
  });
}
function copyBoth(btn){
  const pair = QUERIES[tkey(curSuite, curRun, curKey)];
  if(!pair) return;
  const sql = compareView === 'sql';
  const suffix = o => sql && o.lang==='preql' ? ' → SQL' : '';
  const block = (who,o) => {
    if(!o) return `-- ===== ${who} (n/a) =====\n(not found)`;
    const err = sql && o.sqlError ? '-- (render error)\n' : '';
    return `-- ===== ${who} (${o.name}${suffix(o)}) =====\n` + err + (sql ? o.sql : o.src);
  };
  const text = block('CANONICAL', pair.canonical) + '\n\n' + block('AGENT', pair.candidate) + '\n';
  const old = btn.textContent;
  writeClip(text).then(()=>{ btn.textContent='Copied ✓'; }).catch(()=>{ btn.textContent='Copy failed'; });
  setTimeout(()=>{ btn.textContent=old; }, 1200);
}

// ---------- replay ----------
const jobKey = (s,r,q) => `${s}|${r}|${q}`;
const runningJobs = () => replays.jobs.filter(j=>j.running);
function jobFor(qid){
  const match = j => j.mode==='one' && j.suite===curSuite && j.run===curRun && j.qid===qid;
  return runningJobs().find(match) || [...replays.jobs].reverse().find(match) || null;
}
function allJob(){
  const running = runningJobs().find(j=>j.mode==='all');
  if(running) return running;
  return [...replays.jobs].reverse().find(j => j.mode==='all' && j.suite===curSuite
    && (j.run===curRun || j.new_run===curRun)) || null;
}
// Questions being replayed right now: their metrics are stale until the job
// lands, so the row pulses instead of showing them.
function isReplaying(q){
  if(q.qid == null) return false;
  for(const j of runningJobs()){
    if(j.suite !== curSuite || (j.run !== curRun && j.new_run !== curRun)) continue;
    if(j.mode === 'all'){
      const p = j.progress || {};
      const act = (p.active && p.active.length) ? p.active : (p.qid != null ? [p.qid] : []);
      if(act.includes(q.qid)) return true;
    } else if(j.qid === q.qid) return true;
  }
  return false;
}
function dropPlaceholders(){
  const jobs = replays.jobs.filter(j=>j.id!=null);
  replays = {running: jobs.some(j=>j.running), jobs};
}
function replayBarHtml(q){
  const job = jobFor(q.qid);
  const all = runningJobs().find(j=>j.mode==='all');
  const mine = !!(job && job.running);
  const blocked = mine || !!(all && all.suite===curSuite && (all.run===curRun || all.new_run===curRun));
  const localErr = localErrors[jobKey(curSuite, curRun, q.qid)];
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
           + ` · ${r.iterations} iters · ${r.duration_seconds}s · pass ${r.pass_count}/${r.num_queries}</span>`;
  }
  return `<button class="cmpbtn" onclick="startReplay(${q.qid})"${blocked?' disabled':''}>`
       + `${esc(mine ? 'Replaying…' : 'Replay ' + qlabel(q.qid))}</button>${status}`;
}
function updateReplayUi(){
  if(page === 'debug' && dbgView === 'drill') renderDrill();
  updateDots();
}
function ensureReplayTimer(){
  if(replays.running && !replayTimer) replayTimer = setInterval(pollReplay, 1500);
}
async function startReplay(qid){
  if(!confirm(`Replay ${qlabel(qid)} in ${curRun}?\n\n`
    + `Re-seeds the semantic model, then re-runs the agent against the current `
    + `prompt and engine.\n\nOVERWRITES this question's trajectory, candidate query `
    + `and report entry. The current trajectory is not recoverable.`)) return;
  const key = jobKey(curSuite, curRun, qid);
  delete localErrors[key];
  // Optimistic placeholder until the server's snapshot answers.
  replays = {running:true, jobs:[...replays.jobs,
    {id:null, running:true, mode:'one', suite:curSuite, run:curRun, qid,
     progress:null, new_run:null, log:['submitting…'], result:null, error:null}]};
  updateReplayUi();
  try{
    const r = await fetch('replay', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({suite: curSuite, run: curRun, qid})});
    const j = await r.json();
    if(r.ok) replays = j;
    else{ dropPlaceholders(); localErrors[key] = j.error || ('HTTP '+r.status); }
  }catch(e){ dropPlaceholders(); localErrors[key] = String(e); }
  updateReplayUi();
  ensureReplayTimer();
}
function rerunAllHtml(){
  if(!served || !(currentIndex()||{}).replayable) return '';
  const job = allJob();
  const on = !!(job && job.running);
  const localErr = localErrors[jobKey(curSuite, curRun, 'all')];
  let status = '', cancel = '';
  if(on){
    const p = job.progress||{};
    const act = (p.active||[]).map(qlabel).join(' ');
    const where = p.total ? `${act || (p.qid!=null ? qlabel(p.qid) : '')} · ${p.done}/${p.total}`
                          : (job.log[job.log.length-1] || 'forking…');
    status = `<span class="rerun-st">${esc(where)}</span>`;
    cancel = `<button class="fbtn cancel" onclick="cancelReplay(${job.id})">Finish in-flight, then stop</button>`;
  } else if(localErr){
    status = `<span class="rerun-st err">${esc(localErr)}</span>`;
  } else if(job && job.error){
    status = `<span class="rerun-st err">${esc(job.error)}</span>`;
  } else if(job && job.result){
    const r = job.result;
    status = `<span class="rerun-st ok">reran ${r.count}/${r.total}${r.errors?` · ${r.errors} errored`:''} · pass ${r.pass_count}/${r.num_queries}</span>`;
  }
  const conc = on ? '' : `<select class="sel" title="parallel agents" onchange="rrConc=+this.value">`
    + [1,2,4].map(n=>`<option value="${n}"${n===rrConc?' selected':''}>${n}×</option>`).join('') + `</select>`;
  return `<button class="fbtn" onclick="startReplayAll()"${on?' disabled':''}>`
       + `${esc(on ? 'Rerunning…' : 'Rerun all → new run')}</button>${conc}${status}${cancel}`;
}
async function startReplayAll(){
  if(!confirm(`Rerun ALL questions in ${curRun}?\n\n`
    + `Copies this run to a fresh sibling dir and re-runs every question there against `
    + `the current prompts, model and engine (${rrConc} at a time). The original run `
    + `is left untouched.\n\nThis is slow - one full agent run per question.`)) return;
  const key = jobKey(curSuite, curRun, 'all');
  delete localErrors[key];
  replays = {running:true, jobs:[...replays.jobs,
    {id:null, running:true, mode:'all', suite:curSuite, run:curRun, qid:null,
     progress:{done:0,total:0,qid:null,active:[]}, new_run:null, log:['forking…'],
     result:null, error:null}]};
  updateReplayUi();
  try{
    const r = await fetch('replay_all', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({suite: curSuite, run: curRun, concurrency: rrConc})});
    const j = await r.json();
    if(r.ok) replays = j;
    else{ dropPlaceholders(); localErrors[key] = j.error || ('HTTP '+r.status); }
  }catch(e){ dropPlaceholders(); localErrors[key] = String(e); }
  updateReplayUi();
  ensureReplayTimer();
}
async function cancelReplay(id){
  try{ await fetch('replay_cancel', {method:'POST', headers:{'Content-Type':'application/json'},
    body: id!=null ? JSON.stringify({id}) : '{}'}); }
  catch(e){ /* the poll will reflect the stop once it lands */ }
}
async function pollReplay(){
  try{
    const r = await fetch('replay_status.json', {cache:'no-store'});
    if(!r.ok) return;
    const j = await r.json();
    const nowRunning = new Set(j.jobs.filter(x=>x.running).map(x=>x.id));
    const finished = runningJobs().filter(x=>x.id!=null && !nowRunning.has(x.id));
    replays = j;
    // A full rerun forks a new dir; follow it live so the fork populates on
    // screen - once per job, so later navigation sticks.
    const fork = j.jobs.find(x => x.mode==='all' && x.new_run && x.suite===curSuite && !followedForks.has(x.id));
    if(fork){
      followedForks.add(fork.id);
      if(fork.new_run !== curRun){ invalidateRun(curSuite, fork.new_run); openRun(curSuite, fork.new_run, null); }
    }
    for(const done of finished) invalidateRun(done.suite, done.run, done.qid != null ? qlabel(done.qid) : null);
    if(finished.length && page==='debug' && dbgView==='drill' && curRun){
      await loadIndex(curSuite, curRun, true).catch(()=>{});
      if(curKey) await loadTrajectory(curSuite, curRun, curKey).catch(()=>{});
    }
    updateReplayUi();
    if(!j.running && replayTimer){ clearInterval(replayTimer); replayTimer = null; }
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
    updateDots();
    ensureReplayTimer();
  }catch(e){ /* static file - no replay */ }
}
function archiveHtml(){
  if(!served || !(currentIndex()||{}).replayable) return '';
  const dis = archiveState.busy ? ' disabled' : '';
  const cls = archiveState.ok ? ' ok' : (archiveState.ok===false ? ' err' : '');
  const status = archiveState.msg ? `<span class="rerun-st${cls}">${esc(archiveState.msg)}</span>` : '';
  return `<button class="fbtn" onclick="archiveRun()"${dis}>`
       + `${esc(archiveState.busy ? 'Archiving…' : 'Archive → history db')}</button>${status}`;
}
async function archiveRun(){
  if(!curRun) return;
  archiveState = {busy:true, msg:'archiving…', ok:null};
  renderDrill();
  try{
    const r = await fetch('archive', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({suite: curSuite, run: curRun})});
    const j = await r.json();
    archiveState = r.ok ? {busy:false, ok:true, msg:`archived ${j.count} questions → ${j.db}`}
                        : {busy:false, ok:false, msg: j.error || ('HTTP '+r.status)};
  }catch(e){ archiveState = {busy:false, ok:false, msg:String(e)}; }
  renderDrill();
}

// ---------- page: evals summary ----------
let summaryData = null;
function showSummary(){ renderSummary(); loadSummary(); }
async function loadSummary(){
  try{
    summaryData = await fetchJSON('summary.json');
    if(page==='evals') renderSummary();
  }catch(e){ /* static file - no summary */ }
}
const pctOf = r => r.total ? Math.round(r.passed/r.total*100) : 0;
function renderSummary(){
  if(!summaryData){ pageEl().innerHTML = spinner('loading summary…'); return; }
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
    const tip = `${r.run} · ${r.passed}/${r.total}` + (r.live ? '' : ' · archived');
    const data = r.live ? ` data-suite="${esc(r.suite)}" data-run="${esc(r.run)}"` : '';
    return `<span class="${cls}" title="${esc(tip)}"${data}>${pctOf(r)}%</span>`;
  };
  let h = `<h1 class="ptitle">Latest performance across evals</h1>`
        + `<div class="pnote">full benchmark runs only · greyed chips are archived (raw logs deleted) · click a live run to debug it</div>`;
  for(const [, s] of bySuite){
    h += `<h2 class="psub">${esc(s.label)}</h2><table class="sum">`
       + `<tr><th>variant</th><th>latest run</th><th>score</th><th>trend (new → old)</th><th>model</th><th>when</th></tr>`;
    for(const [variant, list] of [...s.variants].sort((a,b)=>a[0].localeCompare(b[0]))){
      const latest = list[0];
      const name = latest.live
        ? `<span class="sum-runlink" data-suite="${esc(latest.suite)}" data-run="${esc(latest.run)}">${esc(latest.run)}</span>`
        : `${esc(latest.run)} <span class="sum-muted">(archived)</span>`;
      h += `<tr><td>${esc(variant)}</td><td>${name}</td>`
         + `<td class="sum-score"><b>${latest.passed}/${latest.total}</b> <span class="sum-muted">(${pctOf(latest)}%)</span></td>`
         + `<td>${list.slice(0,8).map((r,i)=>chip(r, i===0)).join('')}</td>`
         + `<td class="sum-muted">${esc(latest.model||'')}</td>`
         + `<td class="sum-muted">${esc(fmtTs(latest.ts))}</td></tr>`;
    }
    h += `</table>`;
  }
  if(!bySuite.size) h += `<div class="meta">no runs found (live or archived)</div>`;
  pageEl().innerHTML = h;
  pageEl().querySelectorAll('[data-run]').forEach(n=>{
    n.onclick = ()=> openRun(n.dataset.suite, n.dataset.run, null);
  });
}

// ---------- page: launch ----------
// Every control maps to a run_eval.py flag; the command line above the button
// is rendered by the server from the same builder that runs it.
const LF_STORE = 'evalLaunchForms';
function loadForms(){ try{ return JSON.parse(localStorage.getItem(LF_STORE) || '{}'); }catch(e){ return {}; } }
function saveForms(){ try{ localStorage.setItem(LF_STORE, JSON.stringify(launchForms)); }catch(e){ /* private mode */ } }
launchForms = loadForms();

const suiteOpts = key => (launchOpts ? launchOpts.suites.find(s=>s.key===key) : null) || null;
function defaultForm(s){
  const d = s.defaults;
  return {categories: s.base.slice(), num_queries: d.num_queries, query_ids: '', splice: false,
          scale_factor: d.scale_factor, provider: d.provider, model: d.model, reasoning_effort: '',
          concurrency: d.concurrency, max_iterations: d.max_iterations, timeout: d.timeout,
          enable_todo: false};
}
// The stored form is merged over the suite's defaults, so a spec default that
// moves (or a category that disappears) doesn't leave a stale form behind.
function formFor(key){
  const s = suiteOpts(key);
  if(!s) return null;
  const f = {...defaultForm(s), ...(launchForms[key] || {})};
  f.categories = (f.categories||[]).filter(c=>s.categories.some(x=>x.key===c));
  if(!f.categories.length) f.categories = s.base.slice();
  launchForms[key] = f;
  return f;
}
function countIds(raw){
  let n = 0;
  for(const part of String(raw||'').trim().split(/[,\s]+/)){
    if(!part) continue;
    const m = /^(\d+)\s*-\s*(\d+)$/.exec(part);
    n += m ? Math.max(0, +m[2] - +m[1] + 1) : 1;
  }
  return n;
}
function setField(key, value, rerender){
  const f = formFor(launchSuite);
  if(!f) return;
  f[key] = value; saveForms();
  if(rerender) renderLaunch(); else queuePreview();
}
function toggleCategory(key){
  const f = formFor(launchSuite), order = suiteOpts(launchSuite).categories.map(c=>c.key);
  const i = f.categories.indexOf(key);
  if(i>=0) f.categories.splice(i,1); else f.categories.push(key);
  f.categories.sort((a,b)=>order.indexOf(a)-order.indexOf(b));
  saveForms(); renderLaunch();
}
function pickCategories(which){
  const s = suiteOpts(launchSuite), f = formFor(launchSuite);
  const of = pred => s.categories.filter(pred).map(c=>c.key);
  f.categories = which==='base' ? s.base.slice()
    : which==='trilogy' ? of(c=>c.harness==='trilogy')
    : which==='sql' ? of(c=>c.harness==='sql')
    : which==='all' ? of(()=>true) : [];
  saveForms(); renderLaunch();
}
function launchOnSuite(key){ launchSuite = key; renderLaunch(); }
function queuePreview(){ clearTimeout(previewTimer); previewTimer = setTimeout(refreshPreview, 300); }
async function refreshPreview(){
  const f = formFor(launchSuite);
  if(!f) return;
  saveForms();
  try{
    const r = await fetch('launch', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({...f, suite: launchSuite, preview: true})});
    const j = await r.json();
    launchPreview = r.ok ? j.command : '';
    launchError = r.ok ? '' : (j.error || ('HTTP '+r.status));
  }catch(e){ launchPreview = ''; launchError = String(e); }
  updatePreview();
}
function updatePreview(){
  const prev = document.getElementById('lprev');
  if(prev) prev.innerHTML = launchPreview ? `<pre class="lcmd">${esc(launchPreview)}</pre>` : '';
  const err = document.getElementById('lerr');
  if(err) err.innerHTML = launchError ? `<div class="rerun-st err">${esc(launchError)}</div>` : '';
  const btn = document.getElementById('lgo');
  if(btn) btn.disabled = launchBusy || !launchPreview;
}
async function loadLaunchOptions(){
  try{
    launchOpts = await fetchJSON('launch_options.json');
    if(!suiteOpts(launchSuite)) launchSuite = (launchOpts.suites[0]||{}).key || null;
    if(page==='launch') renderLaunch();
  }catch(e){ launchError = String(e.message||e); if(page==='launch') renderLaunch(); }
}
function showLaunch(){
  if(!launchSuite) launchSuite = curSuite;
  renderLaunch();
  if(!launchOpts) loadLaunchOptions();
  pollLaunch();
}
async function submitLaunch(){
  const f = formFor(launchSuite), s = suiteOpts(launchSuite);
  if(!f || !launchPreview) return;
  const scope = f.query_ids.trim() ? `questions ${f.query_ids.trim()}` : `the first ${f.num_queries} questions`;
  if(!confirm(`Launch ${f.categories.length} leg(s) over ${scope} on ${s.label}?\n\n${launchPreview}\n\n`
    + `This spends real API budget and can run for hours.`)) return;
  launchBusy = true; updatePreview();
  try{
    const r = await fetch('launch', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({...f, suite: launchSuite})});
    const j = await r.json();
    if(r.ok){ launchJobs = j; launchError = ''; } else launchError = j.error || ('HTTP '+r.status);
  }catch(e){ launchError = String(e); }
  launchBusy = false;
  renderLaunch();
}
async function cancelLaunch(id){
  if(!confirm('Stop this run?\n\nThe eval process and every agent under it are killed. '
    + 'Artifacts written so far stay on disk.')) return;
  try{
    const r = await fetch('launch_cancel', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id})});
    if(r.ok) launchJobs = await r.json();
  }catch(e){ /* the poll will reflect it */ }
  renderLaunchJobs();
}
async function pollLaunch(){
  try{
    const r = await fetch('launch_status.json', {cache:'no-store'});
    if(!r.ok) return;
    const before = launchJobs.jobs.filter(j=>j.state==='running').map(j=>j.id);
    launchJobs = await r.json();
    updateDots();
    if(page==='launch') renderLaunchJobs();
    // A run that just finished wrote new result dirs; let the grid pick them up.
    const now = new Set(launchJobs.jobs.filter(j=>j.state==='running').map(j=>j.id));
    if(before.some(id => !now.has(id))) delete MATRIX[curSuite];
  }catch(e){ /* static file - no launching */ }
}
function updateDots(){
  const l = document.getElementById('dot-launch');
  if(l) l.className = 'rib-dot' + (launchJobs.running ? ' on' : '');
  const d = document.getElementById('dot-debug');
  if(d) d.className = 'rib-dot' + (replays.running ? ' on' : '');
}
function toggleLaunchLog(id){
  if(logOpen.has(id)) logOpen.delete(id); else logOpen.add(id);
  renderLaunchJobs();
}
function launchFormHtml(s, f){
  const num = n => `<input class="lin num" type="number" value="${esc(String(f[n]))}" oninput="setField('${n}', this.value)">`;
  const cats = s.categories.map(c=>{
    const on = f.categories.includes(c.key);
    return `<button class="lchip${on?' on':''}" onclick="toggleCategory('${esc(c.key)}')" `
         + `title="${esc(c.harness)} toolset">${esc(c.key)}<span class="lsub">${esc(c.label)}</span></button>`;
  }).join('');
  const preset = (key,label) => `<button class="fbtn" onclick="pickCategories('${key}')">${esc(label)}</button>`;
  const providers = launchOpts.providers.map(p=>
    `<option value="${esc(p.key)}"${p.key===f.provider?' selected':''}>${esc(p.key)}${p.configured?'':' (no key)'}</option>`).join('');
  const models = launchOpts.models.filter(m=>!f.provider || m.provider===f.provider)
    .map(m=>`<option value="${esc(m.model)}">`).join('');
  const efforts = launchOpts.efforts.map(e=>
    `<option value="${esc(e)}"${e===f.reasoning_effort?' selected':''}>${esc(e||'default')}</option>`).join('');
  const total = s.query_ids.length, ids = f.query_ids.trim();
  const perLeg = ids ? countIds(ids) : Math.min(+f.num_queries || 0, total || +f.num_queries || 0);
  const key = launchOpts.providers.find(p=>p.key===f.provider) || {};
  const keyWarn = key.configured===false
    ? `<div class="rerun-st err">${esc(key.env)} not found in the environment or ${esc(launchOpts.env_file)}</div>` : '';
  const scopeNote = ids ? 'ids override the count' : (total ? `${total} questions available` : 'question list unavailable');
  return `<div class="lform">
    <div class="lrow"><label>Eval</label><div>
      <select class="sel" onchange="launchOnSuite(this.value)">`
      + launchOpts.suites.map(x=>`<option value="${esc(x.key)}"${x.key===s.key?' selected':''}>`
        + `${esc(x.label)}${x.runnable?'':' (no run_eval.py)'}</option>`).join('')
      + `</select>${s.enriched_dir ? '' : '<span class="lnote">no enriched model dir configured</span>'}
    </div></div>
    <div class="lrow"><label>Categories</label><div>
      <div class="lchips">${cats}</div>
      <div class="lpresets">${preset('base','Base funnel')}${preset('trilogy','Trilogy legs')}`
      + `${preset('sql','SQL legs')}${preset('all','All')}${preset('none','None')}</div>
    </div></div>
    <div class="lrow"><label>Questions</label><div class="linline">
      ${num('num_queries')}<span class="lnote">${esc(scopeNote)}</span>
      <input class="lin ids" type="text" placeholder="ids, e.g. 5,13,18 or 1-20"
             value="${esc(f.query_ids)}" oninput="setField('query_ids', this.value)">
      <button class="fbtn" onclick="setField('query_ids','',true)">Clear ids</button>
      ${total ? `<button class="fbtn" onclick="setField('num_queries',${total},true)">All ${total}</button>` : ''}
    </div>${ids ? `<div class="linline"><label class="lcheck"><input type="checkbox"${f.splice?' checked':''} `
      + `onchange="setField('splice', this.checked)"> splice the unrun questions in from the latest run</label></div>` : ''}</div>
    <div class="lrow"><label>Model</label><div class="linline">
      <select class="sel" onchange="setField('provider', this.value, true)">${providers}</select>
      <input class="lin" type="text" list="lmodels" value="${esc(f.model)}" oninput="setField('model', this.value)">
      <datalist id="lmodels">${models}</datalist>
      <span class="lnote">effort</span>
      <select class="sel" onchange="setField('reasoning_effort', this.value)">${efforts}</select>
    </div>${keyWarn}</div>
    <div class="lrow"><label>Knobs</label><div class="linline">
      <span class="lnote">scale factor</span>${num('scale_factor')}
      <span class="lnote">agents per leg</span>${num('concurrency')}
      <span class="lnote">max iters</span>${num('max_iterations')}
      <span class="lnote">timeout s</span>${num('timeout')}
      <label class="lcheck"><input type="checkbox"${f.enable_todo?' checked':''}
        onchange="setField('enable_todo', this.checked)"> todo tool</label>
    </div></div>
    <div class="lrow"><label>Plan</label><div>
      <div class="lplan">${f.categories.length} leg(s) × ${perLeg} question(s) = `
      + `<b>${f.categories.length * perLeg}</b> agent runs · ${esc(String(f.concurrency))} in flight per leg</div>
      <div id="lprev"></div><div id="lerr"></div>
      <button class="fbtn lgo" id="lgo" onclick="submitLaunch()">Launch run</button>
      <span class="lnote">${launchJobs.running ? 'a run is already going - this one queues behind it'
        : 'one run at a time; extra launches queue'}</span>
    </div></div>
  </div>`;
}
function renderLaunch(){
  if(!served){ pageEl().innerHTML = `<div class="meta">Launching needs the served viewer: `
    + `<code>python evals/trajectory_viewer.py --serve 8080</code></div>`; return; }
  if(!launchOpts){
    pageEl().innerHTML = spinner('loading launch options…')
      + (launchError ? `<div class="rerun-st err">${esc(launchError)}</div>` : '');
    return;
  }
  const s = suiteOpts(launchSuite), f = formFor(launchSuite);
  if(!s){ pageEl().innerHTML = `<div class="meta">no runnable eval suites found</div>`; return; }
  pageEl().innerHTML = `<h1 class="ptitle">Launch a run</h1>`
    + `<div class="pnote">runs <code>run_eval.py</code> as a subprocess with the flags below · `
    + `results land in the eval's results dir and show up in the Debug grid</div>`
    + launchFormHtml(s, f)
    + `<h2 class="psub">Launched from here</h2><div id="ljobs"></div>`;
  renderLaunchJobs();
  refreshPreview();
}
function launchBadge(state){
  const cls = state==='done' ? 'pass' : state==='error' ? 'error' : state==='running' ? 'replaying' : 'other';
  return `<span class="badge ${cls}">${esc(state)}</span>`;
}
function renderLaunchJobs(){
  const el = document.getElementById('ljobs');
  if(!el) return;
  const jobs = [...launchJobs.jobs].reverse();
  if(!jobs.length){ el.innerHTML = `<div class="empty">nothing launched from here yet</div>`; return; }
  // A job's log starts expanded while it is live and stays wherever the user
  // last put it - hence "first sighting" rather than "is running".
  for(const j of jobs){
    if(logSeen.has(j.id)) continue;
    logSeen.add(j.id);
    if(j.state==='running' || j.state==='queued') logOpen.add(j.id);
  }
  el.innerHTML = jobs.map(j=>{
    const live = j.state==='running';
    const runs = (j.runs||[]).map(r=> r.ready
      ? `<span class="sum-runlink" data-suite="${esc(j.suite)}" data-run="${esc(r.name)}">${esc(r.name)}</span>`
      : `<span class="sum-muted">${esc(r.name)} (starting…)</span>`).join(' ');
    const stop = (live || j.state==='queued')
      ? `<button class="fbtn cancel" onclick="cancelLaunch(${j.id})">Stop</button>` : '';
    const err = j.error ? `<div class="rerun-st err">${esc(j.error)}</div>` : '';
    return `<div class="ljob${live?' live':''}">
      <div class="ljob-h">${launchBadge(j.state)}<b>${esc(j.label)}</b>
        <span class="sum-muted">${j.elapsed?fmtDur(j.elapsed):''}${j.exit_code!=null?` · exit ${j.exit_code}`:''}</span>
        <span class="grow1"></span>${stop}</div>
      <div class="ljob-cmd">${esc(j.command)}</div>
      ${runs ? `<div class="ljob-runs">${runs}</div>` : ''}${err}
      <div class="head" onclick="toggleLaunchLog(${j.id})"><span class="name">output (${(j.log||[]).length} lines)</span>`
      + `<span class="chev">▼ click to toggle</span></div>
      <pre class="out ljob-log${logOpen.has(j.id)?' show':''}" id="llog${j.id}">${esc((j.log||[]).join('\n'))}</pre>
    </div>`;
  }).join('');
  el.querySelectorAll('[data-run]').forEach(n=>{ n.onclick = ()=>openRun(n.dataset.suite, n.dataset.run, null); });
  for(const j of jobs){
    if(j.state!=='running') continue;
    const pre = document.getElementById('llog'+j.id);
    if(pre) pre.scrollTop = pre.scrollHeight;
  }
}

// ---------- live refresh ----------
// The drilldown re-reads the (cheap) run index and swaps in a trajectory only
// when that question's log actually moved, so a live run streams without
// re-sending the questions that already finished.
async function refreshDrill(){
  if(page!=='debug' || dbgView!=='drill' || !curRun || !served) return;
  // A finished run doesn't change: don't poll it at all.
  const idx = currentIndex();
  if(idx && !replays.running && !idx.questions.some(q => q.status === 'running')) return;
  const stampOf = (idx, key) => {
    const q = idx && idx.questions.find(x => x.key === key);
    return q ? q.stamp + '|' + q.status : '';
  };
  const before = stampOf(currentIndex(), curKey);
  let after;
  try{ after = await loadIndex(curSuite, curRun, true); }catch(e){ return; }
  if(curKey && stampOf(after, curKey) !== before){
    delete TRAJ[tkey(curSuite, curRun, curKey)];
    try{ await loadTrajectory(curSuite, curRun, curKey); }catch(e){ /* keep the old view */ }
  }
  renderDrill();
}

// ---------- boot ----------
// The baked payload is this run's endpoints, precomputed: seeding the caches
// with it means the static file works offline and the served page can open its
// startup run without a single fetch.
function seedPreload(){
  if(!PRELOAD.index || !PRELOAD.run) return;
  const s = PRELOAD.suite || 'preloaded', r = PRELOAD.run;
  curSuite = curSuite || s; curRun = curRun || r;
  INDEX[rkey(s, r)] = PRELOAD.index;
  for(const [k, v] of Object.entries(PRELOAD.trajectories || {})) TRAJ[tkey(s, r, k)] = v;
  for(const [k, v] of Object.entries(PRELOAD.queries || {})) QUERIES[tkey(s, r, k)] = v;
  const first = PRELOAD.index.questions.find(q => q.has_log);
  if(first && !curKey) curKey = first.key;
}
async function loadSuites(){
  try{
    const j = await fetchJSON('suites.json');
    served = true;
    SUITES = j.suites;
    if(!SUITES.some(s => s.key === curSuite)) curSuite = j.current.suite;
    if(!curRun) curRun = j.current.run;
  }catch(e){ served = false; }   // opened as a static file
}
seedPreload();
go('debug');
loadSuites().then(()=>{
  showDebug();
  initReplay();
  pollLaunch();
});
setInterval(refreshDrill, 5000);
setInterval(()=>{
  if(page==='debug' && dbgView==='grid' && served && !matrixProgress){ delete MATRIX[curSuite]; loadMatrix(curSuite); }
}, 15000);
setInterval(()=>{ if(page==='launch' || launchJobs.running || launchJobs.queued) pollLaunch(); }, 3000);
