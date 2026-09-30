import importlib.util
import json
import os
import re
import shlex
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("gpu_report", Path(__file__).with_name("gpu_benchmark_report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

class ReportTests(unittest.TestCase):
    def test_summary_and_missing_suites(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"BENCHMARK_GPU_VENDORS": "cuda"}):
            root = Path(directory)
            shard = root / "benchmark-results-cuda-gpu-mpi-array"
            shard.mkdir()
            data = {"schema_version": 1, "jobs": [{}], "regressions": [{"name": "x|<y>", "ratio": 2, "metric": "time"}],
                    "improvements": [], "within_noise": [], "insufficient": []}
            (shard / "summary.json").write_text(json.dumps(data))
            body = report.render(root, "build", "abc", "pipeline")
            self.assertIn("| CUDA GPU+MPI | 1 | 0 | 0 | 0 | 1/4 |", body)
            self.assertIn("| CUDA GPU | — | — | — | — | 0/4 |", body)
            self.assertIn("x&#124;&lt;y&gt;", body)
            self.assertIn("linalg (missing or invalid report)", body)
            self.assertLess(body.index("GPU regressions across"), body.index("<details>"))
            data["regressions"][0]["ratio"] = float("nan")
            (shard / "summary.json").write_text(json.dumps(data))
            body = report.render(root, "build", "abc", "pipeline")
            self.assertIn("| CUDA GPU+MPI | — | — | — | — | 0/4 |", body)

    def test_preserves_current_cpu_and_other_pipeline(self):
        cpu = "<!-- dagger-benchmarks -->\n<!-- dagger-benchmark-head:abc -->\nCPU results"
        other = "<!-- dagger-gpu-benchmarks:other:abc -->\nOther GPU\n<!-- /dagger-gpu-benchmarks:other -->"
        stale = "<!-- dagger-gpu-benchmarks:self:abc -->\nOld GPU\n<!-- /dagger-gpu-benchmarks:self -->"
        body = report.merge_comment(cpu + "\n" + other + "\n" + stale, "New GPU", "abc", "self")
        self.assertIn("CPU results", body)
        self.assertIn("Other GPU", body)
        self.assertNotIn("Old GPU", body)
        self.assertTrue(body.endswith("New GPU"))

    def test_discards_previous_commit(self):
        old = "<!-- dagger-benchmarks -->\n<!-- dagger-benchmark-head:old -->\nOld CPU"
        body = report.merge_comment(old, "New GPU", "new", "self")
        self.assertNotIn("Old CPU", body)
        self.assertIn("CPU results for this commit are pending", body)

    def test_only_passed_test_outcomes_start_benchmarks(self):
        script = Path(__file__).with_name("run_gpu_benchmarks.sh").resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".buildkite").mkdir()
            (root / ".buildkite/run_benchmarks.sh").write_text('printf "%s" "$BENCHMARK" > executed')
            agent = root / "buildkite-agent"
            agent.write_text('#!/usr/bin/env bash\nif [[ "$5" == "test-gpu-mpi" ]]; then echo "$MPI_OUTCOME"; else echo "$GPU_OUTCOME"; fi\n')
            agent.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       BENCHMARK_TEST_STEPS="test-gpu,test-gpu-mpi", BENCHMARK_GPU="opencl")
            for first, second, expected in [("passed", "passed", True), ("skipped", "passed", False),
                                             ("passed", "hard_failed", False)]:
                (root / "executed").unlink(missing_ok=True)
                result = subprocess.run(["bash", str(script), "array"], cwd=root,
                    env=dict(env, GPU_OUTCOME=first, MPI_OUTCOME=second), capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((root / "executed").exists(), expected)
                if expected:
                    self.assertEqual((root / "executed").read_text(), "array:dagger+opencl")

    def test_pipeline_matrix_expands_to_valid_suite_arguments(self):
        # Buildkite replaces {{matrix}} once; extra braces survive into argv.
        directory = Path(__file__).parent
        for filename, expected_count in [("pipeline.yml", 12), ("pipeline-julia.yml", 3)]:
            source = (directory / filename).read_text()
            commands = re.findall(r'command: "(bash \.buildkite/run_gpu_benchmarks\.sh [^"\n]+)"', source)
            self.assertEqual(len(commands), expected_count)
            outputs = re.findall(r'BENCHMARK_OUTPUT_DIR: "(benchmark-results-[^"\n]+-gpu[^"\n]*)"', source)
            self.assertEqual(len(outputs), expected_count)
            for suite in ("array", "linalg", "sparse", "stencil"):
                for command in commands:
                    with self.subTest(pipeline=filename, command=command, suite=suite):
                        expanded = command.replace("{{matrix}}", suite)
                        self.assertEqual(shlex.split(expanded),
                                         ["bash", ".buildkite/run_gpu_benchmarks.sh", suite])
                for output in outputs:
                    expanded = output.replace("{{matrix}}", suite)
                    self.assertTrue(expanded.endswith("-" + suite))
                    self.assertNotRegex(expanded, r"[{}]")

    def test_bounds_details(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"BENCHMARK_GPU_VENDORS": "metal"}):
            root = Path(directory)
            shard = root / "benchmark-results-metal-gpu-array"
            shard.mkdir()
            data = {"schema_version": 1, "jobs": [{}], "regressions": [{"name": str(n), "ratio": 2, "metric": "time"} for n in range(1000)],
                    "improvements": [], "within_noise": [], "insufficient": []}
            (shard / "summary.json").write_text(json.dumps(data))
            body = report.render(root, "build", "abc", "pipeline", limit=5)
            self.assertIn("| Metal GPU | 1000 |", body)
            self.assertIn("Showing 5 of 1000", body)
            self.assertLess(len(body), 60000)

if __name__ == "__main__":
    unittest.main()
