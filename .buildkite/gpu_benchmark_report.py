"""Aggregate GPU shards and append a commit-scoped section to the CPU PR comment."""
import html
import json
import math
import os
from pathlib import Path
import re
import urllib.error
import urllib.request

CATEGORIES = ("regressions", "improvements", "within_noise", "insufficient")
SUITES = ("array", "linalg", "sparse", "stencil")
VENDORS = {"cuda": "CUDA", "rocm": "ROCm", "oneapi": "oneAPI", "metal": "Metal", "opencl": "OpenCL"}
VARIANTS = {"gpu": "GPU", "gpu-distributed": "GPU+Distributed", "gpu-mpi": "GPU+MPI"}
MARKER = "<!-- dagger-benchmarks -->"


def escape(value):
    return html.escape(str(value)).replace("|", "&#124;").replace("`", "&#96;").replace("\n", " ")


def render(root, url, commit, section, limit=100):
    groups = []
    expected = os.environ.get("BENCHMARK_GPU_VENDORS", "").split(",")
    for vendor, label in VENDORS.items():
        if expected != [""] and vendor not in expected:
            continue
        for variant, topology in VARIANTS.items():
            shards = []
            vendor_suites = SUITES if vendor != "oneapi" else SUITES[:3]
            for suite in vendor_suites:
                file = root / f"benchmark-results-{vendor}-{variant}-{suite}" / "summary.json"
                if not file.exists():
                    continue
                try:
                    data = json.loads(file.read_text())
                    if data["schema_version"] != 1 or not data["jobs"]:
                        raise ValueError("empty or invalid summary")
                    for category in CATEGORIES:
                        for entry in data[category]:
                            if entry["metric"] not in ("time", "allocs", "memory"):
                                raise ValueError("invalid metric")
                            if not isinstance(entry["name"], str) or not math.isfinite(float(entry["ratio"])):
                                raise ValueError("invalid entry")
                    shards.append((suite, data))
                except (ValueError, KeyError, TypeError) as exc:
                    shards.append((suite, {"error": str(exc)}))
            if shards or vendor in expected:
                groups.append((f"{label} {topology}", shards, vendor_suites))
    lines = [f"<!-- dagger-gpu-benchmarks:{section}:{commit} -->", "## GPU benchmarks", "",
             "Timing regressions require five samples per revision and independent confirmation.", "",
             "| Backend | Regressions | Improvements | Within noise | Inconclusive time | Suites |",
             "|:---|---:|---:|---:|---:|:---|"]
    totals = {key: [] for key in CATEGORIES}
    for label, shards, vendor_suites in groups:
        valid = [(suite, data) for suite, data in shards if "error" not in data]
        counts = [sum(len(data[key]) for _, data in valid) for key in CATEGORIES]
        cells = " | ".join(map(str, counts)) if valid else "— | — | — | —"
        lines.append(f"| {label} | {cells} | {len(valid)}/{len(vendor_suites)} |")
        for key in CATEGORIES:
            totals[key].extend((label, entry) for _, data in valid for entry in data[key])
    if not groups:
        lines.extend(["", "No GPU benchmark summaries were produced; test gates or worker failures may have prevented execution."])
    def changes(entries):
        if not entries:
            return ["_None in available results._"]
        table = ["| Backend | Benchmark | Metric | Change |", "|:---|:---|:---|---:|"]
        for label, entry in sorted(entries, key=lambda item: -abs(item[1]["ratio"] - 1))[:limit]:
            table.append(f"| {label} | `{escape(entry['name'])}` | {escape(entry['metric'])} | {(entry['ratio'] - 1)*100:+.1f}% |")
        if len(entries) > limit:
            table.append(f"\n_Showing {limit} of {len(entries)} changes; see summary artifacts for complete lists._")
        return table
    for category in ("regressions", "improvements"):
        lines.extend(["", f"### GPU {category} across all backends", "", *changes(totals[category])])
    for label, shards, vendor_suites in groups:
        lines.extend(["", "<details>", f"<summary>{label}</summary>", "",
                      "| Suite | Regressions | Improvements | Within noise | Inconclusive time |",
                      "|:---|---:|---:|---:|---:|"])
        by_suite = dict(shards)
        for suite in vendor_suites:
            data = by_suite.get(suite)
            if data is None or "error" in data:
                lines.append(f"| {suite} (missing or invalid report) | — | — | — | — |")
            else:
                lines.append(f"| {suite} | " + " | ".join(str(len(data[key])) for key in CATEGORIES) + " |")
        for category in ("regressions", "improvements"):
            entries = [(label, entry) for _, data in shards if "error" not in data for entry in data[category]]
            if entries:
                lines.extend(["", f"#### {category.capitalize()}", "", *changes(entries)])
        lines.extend(["", "</details>"])
    lines.extend(["", f"[GPU results and plots on Buildkite]({url})", f"<!-- /dagger-gpu-benchmarks:{section} -->"])
    return "\n".join(lines)


def merge_comment(existing, gpu, commit, section):
    # Replace this pipeline's section; preserve other pipelines only for this SHA.
    pattern = r"<!-- dagger-gpu-benchmarks:([^:]+):([^ ]+) -->.*?<!-- /dagger-gpu-benchmarks:\1 -->"
    def keep(match):
        return match.group(0) if match[1] != section and match[2] == commit else ""
    existing = re.sub(pattern, keep, existing, flags=re.S).rstrip()
    # Never append current GPU results beneath CPU results from another commit.
    head = re.search(r"<!-- dagger-benchmark-head:([^ ]+) -->", existing)
    if head is None or head[1] != commit:
        existing = f"{MARKER}\n<!-- dagger-benchmark-head:{commit} -->\nCPU results for this commit are pending."
    return existing + "\n\n" + gpu


def publish(report, commit, section):
    token = os.environ.get("GITHUB_TOKEN")
    pr = os.environ.get("BUILDKITE_PULL_REQUEST", "false")
    if not token or pr == "false":
        print("GitHub comment skipped: no PR write token; report remains in Buildkite artifacts/annotation.")
        return
    repo = re.sub(r".*github.com[:/]", "", os.environ["BUILDKITE_REPO"]).removesuffix(".git")
    base = f"https://api.github.com/repos/{repo}/issues/{pr}/comments"
    def request(url, method="GET", payload=None):
        req = urllib.request.Request(url, method=method, data=None if payload is None else json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    pull = request(f"https://api.github.com/repos/{repo}/pulls/{pr}")
    if pull["head"]["sha"] != commit:
        print("GitHub comment skipped: this build is no longer the PR head.")
        return
    comments = []
    for page in range(1, 100):
        batch = request(f"{base}?per_page=100&page={page}")
        comments.extend(batch)
        if len(batch) < 100:
            break
    existing = next((c for c in comments if MARKER in (c.get("body") or "")), None)
    body = merge_comment(existing["body"] if existing else "", report, commit, section)
    if len(body) > 60000:
        # Keep the CPU comment intact when combined results exceed GitHub's limit.
        body = report
        existing = next((c for c in comments if report.splitlines()[0] in (c.get("body") or "")), None)
    if existing:
        try:
            request(f"https://api.github.com/repos/{repo}/issues/comments/{existing['id']}", "PATCH", {"body": body})
        except urllib.error.HTTPError as exc:
            if exc.code not in (403, 404):
                raise
            request(base, "POST", {"body": report})
    else:
        request(base, "POST", {"body": body})


if __name__ == "__main__":
    import sys
    root = Path(sys.argv[1])
    commit = os.environ["BUILDKITE_COMMIT"]
    section = os.environ.get("BUILDKITE_PIPELINE_SLUG", "gpu")
    report = render(root, os.environ.get("BUILDKITE_BUILD_URL", ""), commit, section)
    if len(report) > 50000:
        report = render(root, os.environ.get("BUILDKITE_BUILD_URL", ""), commit, section, limit=5)
    (root / "report.md").write_text(report)
    try:
        publish(report, commit, section)
    except Exception as exc:
        print(f"GitHub comment failed (report retained): {exc}")
