const test = require('node:test');
const assert = require('node:assert/strict');
const { mergeCPUReport } = require('./benchmark-comment.cjs');
test('CPU publication preserves current GPU sections below CPU results', () => {
  const section = '<!-- dagger-gpu-benchmarks:pipeline:abc -->\nGPU results\n<!-- /dagger-gpu-benchmarks:pipeline -->';
  const body = mergeCPUReport('<!-- dagger-benchmarks -->\nCPU results', section, 'abc');
  assert.ok(body.includes('<!-- dagger-benchmark-head:abc -->'));
  assert.ok(body.indexOf('CPU results') < body.indexOf('GPU results'));
  assert.ok(!mergeCPUReport('<!-- dagger-benchmarks -->\nCPU results', section, 'new').includes('GPU results'));
});
test('CPU result stays within comment limit', () => {
  const section = '<!-- dagger-gpu-benchmarks:pipeline:abc -->' + 'x'.repeat(1000) + '<!-- /dagger-gpu-benchmarks:pipeline -->';
  assert.ok(mergeCPUReport('<!-- dagger-benchmarks -->' + 'c'.repeat(59500), section, 'abc').length <= 60000);
});
