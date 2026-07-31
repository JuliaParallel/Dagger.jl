# Tuolumne MI300a one-shot benchmark

One Julia session produces every number the paper needs for Tuolumne coverage:
single-node (M1), 2-node (M2), and 4-node (M4) MI300a APU regimes, for two
workloads × three tile counts × five schedulers × seven seeds, plus a post-hoc
optimality-gap pass and a schedule-cache demonstration.

You run it **once**. There are no re-runs — validate with `--smoke` first.

---

## 1. Prerequisites (do these on a login node, with network)

1. **Check out the paper branch** at the pinned commit. The script verifies at
   startup that these four scheduler commits are in `HEAD` (matched by subject,
   so a rehash is fine) and aborts with a clear message if any is missing:
   - "stochastic reconstruction for IteratedGreedy"
   - "faithful Ruiz-Stützle acceptance …"
   - "Orsila SA tuning …"
   - "producer-finish term in greedy …"
   plus the MILP-objective instrumentation (`JuMPExt.LAST_MILP_SOLVE`).

2. **ROCm + AMDGPU.jl.** ROCm ≥ 6.0 (MI300a needs a recent ROCm; match what your
   AMDGPU.jl build expects — `AMDGPU.versioninfo()` should show your APUs). The
   script uses `AMDGPU` as the GPU backend and Dagger's ROCExt.

3. **Instantiate the self-contained env** (developer-mode Dagger from this repo):
   ```bash
   julia --project=bench/datadeps_schedulers/tuolumne_env \
         -e 'using Pkg; Pkg.develop(path="."); Pkg.instantiate()'
   ```
   After this, launch with **no** `--project` — the script activates the env
   itself.

---

## 2. Recommended allocation

- **4 MI300a nodes**, **8 hours** wall-time (≈6 h expected; the margin covers
  cold JIT and the 60 s IG/SA/MILP caps that dominate the large-K cells).
- Launch mechanism is **your call** — Flux Framework, Slurm, whatever Tuolumne
  uses. The script does **not** spawn workers; it consumes whatever
  `Dagger.all_processors()` reports and groups them into nodes by hostname, then
  scopes M1/M2/M4 to the first 1/2/4 nodes. Start Julia with one Dagger worker
  per node (each with all local CPU threads + the node's APU GPU visible), added
  through Dagger's DistributedNext so Dagger can see them.
- If fewer than 4 nodes are available, the regimes that don't fit are skipped and
  logged — M1 still runs on a single node.

---

## 3. Run — three steps in order

**a) Smoke test (~30–60 s) — validate the stack before committing the full run:**
```bash
julia bench/datadeps_schedulers/tuolumne_oneshot.jl --smoke
```
It prints the detected topology (nodes / procs / APU GPUs), runs one cholesky
nt=2 cell with RR and Greedy end-to-end, and ends with a single line:
`SMOKE: PASS` or `SMOKE: FAIL`. **Do not proceed unless it PASSes and reports at
least one APU GPU.**

**b) Full grid — RECOMMENDED: partitioned into six Flux jobs.**

The previous single-session run hit the Flux 24 h walltime cap and lost the
matmul cells. The partitioned launcher splits the grid into six independent
Flux jobs, one per (regime × workload), so any single walltime kill loses only
that partition and every other partition keeps going:
```bash
bash bench/datadeps_schedulers/tuolumne_partition_run.sh
```
Monitor with `flux jobs -a`. Each job allocates 4 nodes and 24 h and calls
`tuolumne_oneshot.jl` with `TUOLUMNE_REGIME_FILTER` + `TUOLUMNE_WORKLOAD_FILTER`
env vars set so it only covers that partition's cells. Output CSVs are tagged
with the partition name (e.g. `tuolumne_regime_m1_1node_m1_cholesky.csv`) so
parallel jobs never clobber each other. Post-processing concatenates the
same-schema CSVs across partitions.

Overrides via env vars (all optional):
```bash
NODES=4 TIME_LIMIT=24h FLUX_QUEUE=pbatch \
    bash bench/datadeps_schedulers/tuolumne_partition_run.sh
```

**b-alt) Full grid — single session (fallback, if you have a big walltime slot):**
```bash
julia bench/datadeps_schedulers/tuolumne_oneshot.jl
```
This is what the previous run tried. Progress prints per config/cell so you can
see forward motion. Partial CSVs are flushed every 5 minutes, so a wall-time
expiry won't lose completed work. Any single cell that fails (correctness
residual over threshold, MILP exception, worker crash) is caught, logged, marked
in the CSV, and the run continues. Only use this if you have a 24 h+ Flux
allocation confirmed — otherwise use (b) above.

---

## 4. Output (lands in your invocation directory)

Under the single-session run (b-alt) the CSV / TXT filenames are exactly the
ones below. Under the partitioned run (b, RECOMMENDED) each file is suffixed
with the partition tag (`_m1_cholesky`, `_m1_matmul`, `_m2_cholesky`, …)
so parallel jobs don't clobber each other; concatenate same-schema files
post-hoc.

| File | Contents |
|---|---|
| `tuolumne_regime_m1_1node.csv` | M1 per-cell: wall_ms, aot_ms, residual, n_tasks, milp_status, milp_obj, bs, n_nodes |
| `tuolumne_regime_m2_2node.csv` | M2 per-cell (same columns) |
| `tuolumne_regime_m4_4node.csv` | M4 per-cell |
| `tuolumne_medians.csv` | per-cell medians + stddev over 7 seeds (feeds Table 1) |
| `tuolumne_optgap.csv` | AOT-only Greedy/IG/SA `cost_of_schedule` vs MILP-optimal, with ratio (headline optimality-gap) |
| `tuolumne_cache.csv` | schedule-cache demo: `aot_ms_first_call` vs `aot_ms_cache_hit` per regime |
| `tuolumne_run_summary.txt` | git SHA, launch/elapsed times, failure count, flagged cells, full log |

Notes:
- **bs**: 4096 for nt ∈ {2,4}; nt=8 tries 4096 first (MI300a's 128 GB unified
  HBM3 may fit it) and falls back to 2048 on OOM — the actual bs is recorded per
  cell and logged.
- **optgap**: only cells where MILP proved `OPTIMAL` (small K) contribute; that's
  by design — a time-limited MILP is not a valid lower bound.
- **cache**: `aot_ms_cache_hit` should be ≈0 (structural-equivalence lookup). If
  the two columns are similar, the cache isn't hitting on your Julia version —
  flag it to us.

---

## 5. What to send back

All seven files above. If the smoke test failed, send `tuolumne_run_summary.txt`
(or the console output) and your `AMDGPU.versioninfo()` / ROCm version so we can
diagnose the stack before you spend the allocation.
