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

## 3. Run — two commands, in order

**a) Smoke test (~30–60 s) — validate the stack before committing the full run:**
```bash
julia bench/datadeps_schedulers/tuolumne_oneshot.jl --smoke
```
It prints the detected topology (nodes / procs / APU GPUs), runs one cholesky
nt=2 cell with RR and Greedy end-to-end, and ends with a single line:
`SMOKE: PASS` or `SMOKE: FAIL`. **Do not proceed unless it PASSes and reports at
least one APU GPU.**

**b) Full grid (~6–8 h):**
```bash
julia bench/datadeps_schedulers/tuolumne_oneshot.jl
```
Progress prints per config/cell so you can see forward motion. Partial CSVs are
flushed every 5 minutes, so a wall-time expiry won't lose completed work. Any
single cell that fails (correctness residual over threshold, MILP exception,
worker crash) is caught, logged, marked in the CSV, and the run continues.

---

## 4. Output (lands in your invocation directory)

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
