const fs = require('node:fs');
const path = require('node:path');

const marker = '<!-- dagger-benchmarks -->';
const suites = ['array', 'linalg', 'sparse', 'stencil'];
const backends = [
  { id: 'default', label: 'Threads', topology: '1 process × 4 threads' },
  { id: 'distributed', label: 'Distributed', topology: '4 processes × 1 thread' },
  { id: 'distributed-threads', label: 'Distributed+Threads', topology: '2 processes × 2 threads' },
  { id: 'mpi', label: 'MPI', topology: '4 ranks × 1 thread' },
  { id: 'mpi-threads', label: 'MPI+Threads', topology: '2 ranks × 2 threads' },
];
const categories = ['regressions', 'improvements', 'within_noise', 'insufficient'];
const escape = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;').replaceAll('|', '&#124;').replaceAll('`', '&#96;').replaceAll('\n', ' ');

function readBackend(root, backend) {
  const shards = suites.map(suite => {
    const file = path.join(root, `benchmark-results-${backend.id}-${suite}`, 'summary.json');
    if (!fs.existsSync(file)) return { suite, error: 'missing report' };
    try {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (data.schema_version !== 1 || !Array.isArray(data.jobs) || !data.jobs.length ||
          !categories.every(key => Array.isArray(data[key]) && data[key].every(entry =>
            typeof entry.name === 'string' && ['time', 'allocs', 'memory'].includes(entry.metric) &&
            Number.isFinite(entry.ratio)))) throw new Error('invalid summary');
      return { suite, data };
    } catch {
      return { suite, error: 'invalid report' };
    }
  });
  const entries = Object.fromEntries(categories.map(key => [key,
    shards.flatMap(shard => (shard.data?.[key] ?? []).map(entry => ({ ...entry, backend: backend.label }))),
  ]));
  return { ...backend, shards, ...entries, available: shards.filter(shard => shard.data).length };
}

function countCells(data) {
  return categories.map(key => data ? data[key].length : '—').join(' | ');
}

function changesTable(entries, limit, showBackend = true) {
  if (!entries.length) return '_None in the available results._';
  const rows = [
    `| ${showBackend ? 'Backend | ' : ''}Benchmark | Metric | Change |`,
    `| ${showBackend ? ':---| ' : ''}:---|:---|---:|`,
  ];
  for (const entry of entries.slice(0, limit)) {
    const percent = (entry.ratio - 1) * 100;
    rows.push(`| ${showBackend ? escape(entry.backend) + ' | ' : ''}\`${escape(entry.name)}\` | ${escape(entry.metric)} | ${percent > 0 ? '+' : ''}${percent.toFixed(1)}% |`);
  }
  if (entries.length > limit) {
    rows.push('', `_Showing ${limit} of ${entries.length} changes; the complete lists are in the summary.json artifacts._`);
  }
  return rows.join('\n');
}

function sortChanges(entries, improvement = false) {
  return [...entries].sort((a, b) => (improvement ? a.ratio - b.ratio : b.ratio - a.ratio) ||
    a.backend.localeCompare(b.backend) || a.name.localeCompare(b.name) || a.metric.localeCompare(b.metric));
}

function renderReport(results, runUrl, limit) {
  const parts = [marker, '## Dagger benchmarks: `dirty` vs `master`', '',
    'Counts are benchmark metrics (time, allocations, bytes). Timing changes require non-overlapping median ± IQR bands; single-sample timing changes are inconclusive.', '',
    '| Backend | Regressions | Improvements | Within noise | Inconclusive time | Suites |',
    '|:---|---:|---:|---:|---:|:---|'];
  for (const backend of results) {
    parts.push(`| ${backend.label} | ${countCells(backend.available ? backend : null)} | ${backend.available}/${suites.length} |`);
  }
  const totals = Object.fromEntries(categories.map(key => [key, results.flatMap(backend => backend[key])]));
  const available = results.reduce((n, backend) => n + backend.available, 0);
  parts.push(`| **Total** | ${countCells(available ? totals : null)} | ${available}/${suites.length * results.length} |`);
  const missing = results.flatMap(backend => backend.shards.filter(shard => shard.error)
    .map(shard => `${backend.label}/${shard.suite} (${shard.error})`));
  if (missing.length) parts.push('', `**Incomplete results:** ${missing.join(', ')}. Counts cover available suites only.`);
  parts.push('', '### Regressions across all backends', '',
    changesTable(sortChanges(totals.regressions), limit), '',
    '### Improvements across all backends', '',
    changesTable(sortChanges(totals.improvements, true), limit));

  for (const backend of results) {
    parts.push('', '<details>', `<summary>${backend.label} (${backend.topology}) — ${backend.available ? `${backend.regressions.length} regressions, ${backend.improvements.length} improvements` : 'results unavailable'}</summary>`, '',
      '| Suite | Regressions | Improvements | Within noise | Inconclusive time |',
      '|:---|---:|---:|---:|---:|');
    for (const shard of backend.shards) {
      parts.push(`| ${shard.suite}${shard.error ? ` (${shard.error})` : ''} | ${countCells(shard.data)} |`);
    }
    for (const [key, title] of [['regressions', 'Regressions'], ['improvements', 'Improvements'],
                               ['within_noise', 'Within noise'], ['insufficient', 'Inconclusive timing changes']]) {
      if (!backend[key].length) continue;
      parts.push('', `#### ${title}`, '', changesTable(sortChanges(backend[key], key === 'improvements'), limit, false));
    }
    parts.push('', '</details>');
  }
  parts.push('', `[Full results and plots](${runUrl}) (download the \`benchmark-results-*\` artifacts).`);
  return parts.join('\n');
}

function buildReport(root, runUrl) {
  const results = backends.map(backend => readBackend(root, backend));
  const full = renderReport(results, runUrl, Infinity);
  if (full.length <= 60000) return full;
  // GitHub limits comment bodies to 65,536 characters. Preserve totals and
  // coverage even in a very noisy run, and explicitly label any shortened list.
  for (let limit = 256; limit >= 1; limit = Math.floor(limit / 2)) {
    const report = renderReport(results, runUrl, limit);
    if (report.length <= 60000) return report;
  }
  throw new Error('Benchmark summary exceeds the comment size limit');
}

module.exports = { buildReport, marker, backends, suites };
