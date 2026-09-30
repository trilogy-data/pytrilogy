// Headless check for viewer.html: stub the DOM, load a trace, render every step.
//   node local_scripts/plan_debugger/check_viewer.js local_scripts/plan_debugger/viewer.html <trace.json>
const fs = require('fs');
const [,, viewerPath, tracePath] = process.argv;
const html = fs.readFileSync(viewerPath, 'utf8');
const script = html.match(/<script>([\s\S]*)<\/script>/)[1];
const trace = JSON.parse(fs.readFileSync(tracePath, 'utf8'));

function el(tag = 'div') {
  return {
    tagName: tag.toUpperCase(), dataset: {}, children: [], _html: '', textContent: '', style: {},
    classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
    set innerHTML(v) { this._html = String(v); }, get innerHTML() { return this._html; },
    querySelector(sel) { return el(sel); },
    querySelectorAll() { return []; },
    addEventListener() {}, setAttribute() {}, getBoundingClientRect() { return { width: 1000 }; },
    scrollIntoView() {}, closest() { return null; }, click() {},
  };
}
const elements = {};
const document = {
  querySelector: sel => {
    // the real DOM has no #graph on a step whose view did not render one
    if (sel === '#graph' && !(elements['#view'] && elements['#view'].innerHTML.includes('id="graph"'))) return null;
    return elements[sel] || (elements[sel] = el());
  },
  querySelectorAll: () => [],
  getElementById: () => null,
  addEventListener() {},
};
const window = { addEventListener() {} };
const location = { hash: '', search: '' };
const history = { replaceState() {} };
const fetch = () => Promise.reject(new Error('no fetch'));
const URLSearchParams = class { get() { return null; } };
new Function('document', 'window', 'location', 'history', 'fetch', 'URLSearchParams', 'trace', script + `
  load(trace);
  let rendered = 0, chars = 0;
  const seen = {};
  for (S.idx = 0; S.idx < S.steps.length; S.idx++) {
    show();
    const out = document.querySelector('#view').innerHTML;
    if (out.length < 40) throw new Error('empty render for step ' + S.steps[S.idx].seq);
    seen[S.steps[S.idx].phase] = (seen[S.steps[S.idx].phase] || 0) + 1;
    rendered++; chars += out.length;
  }
  // collapsing the outermost block hides its steps; stepping into it opens it again
  S.idx = 0; applyFilter();
  const count = () => (document.querySelector('#steps').innerHTML.match(/class="step /g) || []).length;
  const first = S.chains.findIndex(c => c.length);
  if (first >= 0) {
    const all = count(); S.collapsed.add(S.chains[first][0].key); renderSteps();
    if (count() >= all) throw new Error('collapsing a block hid no steps');
    S.idx = first; show();
    if (count() !== all) throw new Error('stepping into a collapsed block did not open it');
  }
  S.plan = trace.plans[trace.plans.length - 1].id; applyFilter();
  S.plan = 'all'; S.phases.delete('grouping'); applyFilter();
  console.log('rendered', rendered, 'steps,', chars, 'chars of HTML', JSON.stringify(seen));
`).call(null, document, window, location, history, fetch, URLSearchParams, trace);
