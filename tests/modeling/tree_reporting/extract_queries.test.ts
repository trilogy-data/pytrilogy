// Regenerates queries.json from the app's own query catalog
// (src/src/tests/dashboardQueryCatalog.ts in greenmtnboy/tree_reporting), so
// the ids, filters and cross-filter binds are the ones the pages send.
//
// Run from an app checkout's `src/` with a vitest config whose `test.dir` is
// this directory and `test.include` is this file (globals + happy-dom), e.g.
//   APP_SRC=<checkout>/src/src QUERIES_OUT=<this dir>/queries.json APP_COMMIT=<sha> \
//     ./node_modules/.bin/vitest run --config <config>
// The catalog is driven for Boston and the all-cities view: the default state,
// every single-dimension cross-filter, and the species/cultivar selections.
import { writeFileSync } from 'node:fs'

const g = globalThis as { Worker?: unknown }
g.Worker ??= class { postMessage() {} terminate() {} addEventListener() {} removeEventListener() {} }

type Case = { id: string; query: string; imports: Array<{ name: string }>; filters: string[]; parameters?: Record<string, unknown>; state: unknown }

it('extracts the dashboard query catalog', async () => {
  const src = process.env.APP_SRC!
  const ctx = await import(`${src}/composables/dashboardContextSource.ts`)
  const catalog = await import(`${src}/tests/dashboardQueryCatalog.ts`)
  const contexts: Record<string, unknown> = {}
  const seen = new Set<string>()
  const cases: unknown[] = []
  let imports: string[] = []
  for (const city of ['USBOS', null]) {
    const key = city ?? 'ALL'
    contexts[key] = { source: ctx.buildDashboardContextSource(city), parameters: ctx.buildDashboardContextParameters(city) }
    const groups: Array<[string, Case[]]> = [
      ['base', catalog.cityQueryCases(city)],
      ...catalog.crossFilterFieldCombos('all').map((combo: string[]): [string, Case[]] => [`cross_${combo.join('+')}`, catalog.cityCrossFilterCases(city, combo)]),
      ['species_selection', catalog.speciesSelectionCases(city)],
    ]
    for (const [group, c] of groups.flatMap(([name, list]) => list.map((entry): [string, Case] => [name, entry]))) {
      const parameters = c.parameters ?? {}
      const dedupe = JSON.stringify([key, c.query, c.filters, parameters])
      if (seen.has(dedupe)) continue
      seen.add(dedupe)
      imports = c.imports.map((entry) => entry.name)
      cases.push({ city: key, group, id: c.id, query: c.query, filters: c.filters, parameters, state: c.state })
    }
  }
  const source = { repository: 'greenmtnboy/tree_reporting', commit: process.env.APP_COMMIT, note: 'extracted by extract_queries.test.ts; do not edit by hand' }
  writeFileSync(process.env.QUERIES_OUT!, JSON.stringify({ source, imports, contexts, cases }, null, 1) + '\n')
}, 600_000)
