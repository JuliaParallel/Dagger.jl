const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport, backends, suites } = require('./benchmark-report.cjs');

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dagger-report-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}
function write(root, backend, suite, changes = {}) {
  const dir = path.join(root, `benchmark-results-${backend}-${suite}`);
  fs.mkdirSync(dir, { recursive: true });
  const data = { schema_version: 1, jobs: [{ name: `${suite}/dagger` }],
    regressions: [], improvements: [], within_noise: [], insufficient: [], ...changes };
  fs.writeFileSync(path.join(dir, 'summary.json'), JSON.stringify(data));
}
const entry = (name, ratio, metric = 'time') => ({ name, ratio, metric });

test('collates all suites and backends above one dropdown per backend', t => {
  const root = fixture(t);
  for (const backend of backends) for (const suite of suites) write(root, backend.id, suite);
  write(root, 'default', 'array', { regressions: [entry('array/dagger/add', 2)],
    improvements: [entry('array/dagger/sum', 0.5)] });
  write(root, 'distributed', 'linalg', { regressions: [entry('linalg/dagger/lu', 1.8, 'memory')] });
  write(root, 'mpi', 'stencil', { improvements: [entry('stencil/dagger/wrap', 0.4)],
    within_noise: [entry('stencil/dagger/noisy', 1.6)], insufficient: [entry('stencil/dagger/one-sample', 3)] });
  const body = buildReport(root, 'https://example.com/run');
  const overview = body.split('<details>')[0];
  assert.ok(overview.includes(`| **Total** | 2 | 2 | 1 | 1 | ${backends.length * suites.length}/${backends.length * suites.length} |`));
  for (const name of ['array/dagger/add', 'array/dagger/sum', 'linalg/dagger/lu', 'stencil/dagger/wrap']) {
    assert.ok(overview.includes(name), name);
  }
  assert.equal(body.match(/<details>/g).length, backends.length);
  assert.equal(body.match(/<\/details>/g).length, backends.length);
  assert.ok(!body.includes('<summary>array'));
  assert.match(body, /\| array \| 1 \| 1 \| 0 \| 0 \|/);
  assert.match(body, /Distributed \(4 processes × 1 thread\)/);
  assert.match(body, /MPI \(4 ranks × 1 thread\)/);
});

test('missing and invalid shards are visible, never reported as zero regressions', t => {
  const root = fixture(t);
  let body = buildReport(root, 'https://example.com/run');
  assert.match(body, /\| Threads \| — \| — \| — \| — \| 0\/4 \|/);
  assert.match(body, /\*\*Incomplete results:\*\*/);
  write(root, 'mpi', 'sparse');
  write(root, 'default', 'array', { schema_version: 999 });
  body = buildReport(root, 'https://example.com/run');
  assert.match(body, /Threads\/array \(invalid report\)/);
  assert.match(body, /\| MPI \| 0 \| 0 \| 0 \| 0 \| 1\/4 \|/);
  assert.ok(body.includes(`1/${backends.length * suites.length}`));
});

test('escapes benchmark names and bounds oversized comments without losing totals', t => {
  const root = fixture(t);
  const changes = Array.from({ length: 1000 }, (_, n) => entry(`array/dagger/<x|y>\`-${n}`, 2));
  for (const backend of backends) for (const suite of suites) write(root, backend.id, suite, {
    regressions: changes, improvements: changes.map(x => ({ ...x, ratio: 0.5 })),
  });
  const body = buildReport(root, 'https://example.com/run');
  assert.ok(body.length <= 60000);
  const total = 1000 * backends.length * suites.length;
  assert.ok(body.includes(`${total} | ${total}`));
  assert.match(body, new RegExp(`Showing \\d+ of ${total}`));
  assert.match(body, /&lt;x&#124;y&gt;&#96;/);
  assert.equal(body.match(/<details>/g).length, backends.length);
  assert.equal(body.match(/<\/details>/g).length, backends.length);
});
