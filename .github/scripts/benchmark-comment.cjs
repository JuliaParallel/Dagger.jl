const { marker } = require('./benchmark-report.cjs');
function mergeCPUReport(body, existing, head) {
  body = body.replace(marker, `${marker}\n<!-- dagger-benchmark-head:${head} -->`);
  const sections = existing?.match(/<!-- dagger-gpu-benchmarks:[^:]+:[^ ]+ -->[\s\S]*?<!-- \/dagger-gpu-benchmarks:[^ ]+ -->/g) ?? [];
  for (const section of sections) {
    if (section.split('\n')[0].endsWith(`:${head} -->`) && body.length + section.length + 2 <= 60000) body += `\n\n${section}`;
  }
  return body;
}
module.exports = { mergeCPUReport };
