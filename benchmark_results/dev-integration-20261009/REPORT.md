# IPC ownership and asynchronous Datadeps integration validation

Integration is complete and all planned validation controllers have finished, including conditional investigations executed or explicitly skipped by their confirmation rule. Validation was attempted in all 12 backend/topology contexts through composite suites and targeted supplements. Several performance comparisons remain invalid because numerical guards, native crashes, or resource/readiness limits prevented complete runs; this report preserves those failures.

Integrated source revision: `6bc6db556332282bfc15dddb2639663dbedf6ace`. Baseline: `04a8ec2e`. Source branches: IPC `21f59742`, async `b108fb25`. No push or publication.

## Protocol and evidence

Julia 1.11.9; BLAS threads=1; a 4-thread driver; broad Distributed benchmarks use 3 workers with 2 threads each, MPI uses 2 ranks with 4 threads each. Both revisions use matching manifest versions/preferences and the same benchmark scripts. Ten warmups, seven samples, and fresh reversed-order confirmation for flags. Timing gate 10%, allocation gate 25%, with spread checks; allocations are driver/rank0 only. Numerical failures invalidate timings. Broad GPU guards validate eight tiled GEMMs and host/device roundtrips at the configured sizes and both tile widths; they do not check every benchmark output. AMD performance explicitly unsets both serialization flags; AMD correctness uses 3. Intel uses the legacy Level Zero adapter, shared allocation mode 0, and hardware minimum scratch mode equally on both revisions.

Detailed commands and package versions are in [metadata.json](metadata.json), execution status files, and the archived scripts. [performance-finalizer-audit.json](performance-finalizer-audit.json) audits finalizer stderr independently of successful sample/plan checks. Source equivalence is path-audited: older plain/Distributed records remain applicable when only MPI polling and its test changed; earlier MPI timings are historical.

## Correctness

| Backend | Plain | Distributed | MPI on final polling source |
| --- | --- | --- | --- |
| CPU | Default suites recovered across fresh processes; final Datadeps 816, allocations 24 | Default suite plus fresh FFT/tail recovery; final Datadeps 2242/1 expected broken, allocations 24 | 1733 assertions |
| NVIDIA | 246 GPU + 106 stencils; 5 expected broken | 300 GPU + 106 stencils; 4 expected broken | 458 assertions |
| AMD | 246 GPU + 104 stencils; 5 expected broken | 300 GPU + 104 stencils; 4 expected broken | 459 unique assertions in 3 fresh shards; 6 dependency finalizer errors |
| Intel | 246 GPU; 5 expected broken; standard stencil 53 is CPU-only | 300 unique GPU; 4 expected broken in fresh shards; sparse solver shard has reduced process/thread topology | 355 assertions; standard GPU stencils disabled |

Final MPI refresh also passed two focused starvation layouts (48 assertions), one-thread initialized broadcasts/receives, flat/hierarchical async suites, and four-rank one-thread forwarding in both normal and sticky modes (320 assertions). AMD monolithic final-source run hit the host-memory stop policy and is retained as incomplete; its three coverage-preserving shards retain all original assertions, fixtures and 2-rank, 4-thread layout, with duplicate common assertions removed only from coverage accounting.

CPU Distributed originally OOMed at FFT; fresh FFT/tail runs recover remaining default coverage. Intel Distributed requires fresh shards to fit native device/compiler memory, and the sparse solver shard uses a 2-thread driver / one 2-thread worker instead of the standard 4-thread driver / two 2-thread workers. These establish composite coverage, not one clean monolithic run. Final Datadeps test shutdown logs include the runner’s explicit scheduler-halt InterruptException. AMDGPU 2.1.0 destroy_handle! failure was independently reproduced with fake native handles; passing assertions do not repair finalizers.

The explicit Intel GPU stencil supplement below tests the shared stencil definitions plus all 16 exact N=512 / tile=128,256 benchmark wrappers. Its actual outcomes supplement the previously disabled standard GPU cases; failed cases remain uncovered. See [correctness-summary.json](correctness-summary.json) for individual test summaries, exits, stderr audits and source hashes.

CUDA and Intel Distributed broad pairs use identical driver-GPU-pinned untimed capability probes to avoid lost GPU-pointer exceptions during remote reporting. Measured fixtures, scopes and the original 4-thread driver / three 2-thread workers are unchanged. This harness workaround does not repair remote exception transport.

## Required performance matrix

| Context | Broad baseline | Broad integrated | Epoch baseline | Epoch integrated |
| --- | --- | --- | --- | --- |
| cpu-plain | 49 leaves | 49 leaves | 6 leaves | 8 leaves |
| cpu-distributed | 49 leaves | 49 leaves | 6 leaves | 8 leaves |
| cpu-mpi | 47 leaves | 47 leaves | 6 leaves | 8 leaves |
| cuda-plain | 35 leaves | 37 leaves | 6 leaves | 8 leaves |
| cuda-distributed | 35 leaves | 37 leaves | 6 leaves | 8 leaves |
| cuda-mpi | failed (1) | 37 leaves | failed (1) | 8 leaves |
| amdgpu-plain | failed (-6) | failed (1) | 6 leaves | 8 leaves |
| amdgpu-distributed | failed (-15) | failed (-15) | 6 leaves | 8 leaves |
| amdgpu-mpi | failed (124) | failed (124) | failed (1) | 8 leaves |
| oneapi-plain | 35 leaves | 37 leaves | 6 leaves | 8 leaves |
| oneapi-distributed | failed (-11) | 37 leaves | 6 leaves | 8 leaves |
| oneapi-mpi | failed (1) | failed (139) | 6 leaves | 8 leaves |

Failed baseline guards are retained with reuse provenance and produce no baseline/current ratios. NVIDIA/AMD MPI baselines fail foreign-owner MPIRef GPU event recording; Intel MPI baseline fails ambiguous unsafe_convert in IPC handle close. AMD plain native guards fail before timing; the final AMD MPI native guard timed out after 600 seconds while rank0 waited in an HtoD synchronization and rank1 waited for metadata. The final Intel MPI broad guard crashed in SYCL kernel-name/argument-size lookup during SGEMM. The fresh baseline Distributed guard crashed with the same observed SYCL signature, while integrated Distributed passed 37 leaves. This signature occurs before integration and outside MPI; its exact cause remains unresolved and differs from the independently reproduced NEO signal-handler crash. Numerical and resource limitations are distinct from unsupported capability leaves.

## Confirmed flags on applicable source

| Pair | Workload | Metric | First / reversed ratio |
| --- | --- | --- | --- |
| cpu-plain-comparison-final | stencil / dagger / N=512 (block 128) / assign (const) | median_ns | 1.182x / 1.382x |
| epochs-cpu-plain-comparison-final | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-cpu-plain-comparison-final | sync/1 regions/hier=true | allocs | 1.280x / 1.280x |
| epochs-cpu-plain-comparison-final | sync/1 regions/hier=true | bytes | 1.274x / 1.274x |
| epochs-cpu-plain-comparison-final | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-cpu-distributed-comparison | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-cpu-distributed-comparison | sync/8 regions/hier=false | median_ns | 1.133x / 1.164x |
| epochs-cpu-distributed-comparison | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-cuda-plain-comparison | empty/hier=true | allocs | 1.333x / 1.333x |
| epochs-cuda-plain-comparison | empty/hier=true | bytes | 2.021x / 2.021x |
| epochs-cuda-distributed-comparison | empty/hier=true | allocs | 1.333x / 1.333x |
| epochs-cuda-distributed-comparison | empty/hier=true | bytes | 2.021x / 2.021x |
| epochs-amdgpu-plain-comparison | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-amdgpu-plain-comparison | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-amdgpu-distributed-comparison | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-amdgpu-distributed-comparison | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-oneapi-plain-comparison | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-oneapi-plain-comparison | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-oneapi-distributed-comparison | empty/hier=true | allocs | 1.484x / 1.484x |
| epochs-oneapi-distributed-comparison | empty/hier=true | bytes | 2.252x / 2.252x |
| epochs-cpu-mpi-comparison-mpi-pollfix | empty/hier=true | allocs | 1.661x / 1.661x |
| epochs-cpu-mpi-comparison-mpi-pollfix | empty/hier=true | median_ns | 1.364x / 1.343x |
| epochs-cpu-mpi-comparison-mpi-pollfix | empty/hier=true | bytes | 2.481x / 2.481x |
| epochs-oneapi-mpi-comparison-mpi-pollfix | empty/hier=true | allocs | 1.677x / 1.677x |
| epochs-oneapi-mpi-comparison-mpi-pollfix | empty/hier=true | bytes | 2.496x / 2.496x |

The original CPU constant-stencil tile 128 time flag survives reversed broad runs (1.18x/1.38x), but four fresh fixed-input profiles reproduce no gate-crossing flag at tile 128 or 256, under either arbitrary placement or worker 1 / thread 1 pinning. All profiles use the same numerical fixture, CPU affinity, ten warmups and seven GC samples. This preserves the original observation while showing its dependence on runtime/placement/history. See small-profile-comparison.json for raw comparisons and allocation frames. Synchronous Distributed epoch timing and hierarchical bookkeeping allocation flags remain confirmed. Empty flat planning has already been improved: integrated pre-fastpath 322 allocations/17,760 bytes to 90/4,688; hierarchical 101/5,168 to 90/4,688, with prior-write/failure barriers retained. Epoch harness overhead produces 92/4,720 for the corresponding final empty cases.

An MPI polling change initially made Intel eight-region synchronous epochs 44–47 ms versus baseline 10–11 ms. Runtime controls and actual oneArray argument checks attributed this to unconditional Julia timer parking; final production conditional backoff restored 10.4–10.9 ms in the numerical epoch probe. Sticky tasks still park through Julia sleep; nonsticky tasks use 100 μs OS backoff/yield. Ready-message allocation counts remain 37,946/1,799,136 bytes per 1000 before/after, warmup 10 / minimum of 7. Final-source Intel MPI paired epochs and their independent reversed pair passed numerical/device-argument checks with seven samples each; the prior synchronous latency flags no longer confirm. Current eight synchronous regions take 10.039 ms flat and 10.932 ms hierarchical. Ready-receive allocation counts remain unchanged and focused sticky starvation plus four-rank forwarding checks pass.

Eight asynchronous regions generally save allocations against eight current synchronous regions. AMD Distributed is a measured tradeoff: repeated async 108 ms versus sync 81 ms, despite roughly 33–36% lower driver allocations; the native profile outcomes are recorded below. Treat this workload separately from faster asynchronous results on other backends.

## Supplements and unresolved limitations

Independent host-SPD GPU Cholesky has completed all 18 revision/context execution records: 10 cases passed 2 leaves/seven samples/device-type and numerical reconstruction checks. Four complete pairs (NVIDIA and Intel, plain and Distributed) have no gate flags. Integrated NVIDIA MPI and AMD Distributed pass without valid baselines. AMD Distributed baseline never published a plan before the 600-second readiness deadline despite its passed guard; its bounded untimed profiles show parked driver threads but do not establish the cause. Other rejected guards are reused with provenance. Intel Distributed baseline additionally logs a closed-stream warning during guard teardown; all guarded factorization measurements and exit 0 are retained with that stderr caveat. See cholesky-completion-summary.json.

Both CPU FFT widths have completed in plain, Distributed and final-source MPI contexts. Four fixed-input CPU stencil profiles have also completed. The large Distributed array/stencil group completed 30 paired leaves without flags; tile 128 linear algebra completed 8 paired leaves plus two fresh reversed runs without confirmed flags. Its initial SVD 1.175x timing flag did not confirm (reversed1.071x). Both 64-width large linear-algebra groups remain incomplete because an SVD worker segfaulted. Baseline returned 7 of 8 leaves; integrated returned 3 of 8 and its survivors were stopped after bounded diagnostics. systemd-coredump independently confirms both crashes; the observed baseline Julia deserialize stack and integrated unnamed JIT/native stack do not establish one shared cause. The fresh seven-leaf comparison below preserves the unaffected operations and retains SVD as failed coverage. [performance/completion-audit.json](performance/completion-audit.json) tracks each required comparison; [performance/validation-summary.json](performance/validation-summary.json) keeps complete findings and independent confirmation evidence.

The standalone C++ HIP control completed: native trial 0 returned 0/0 instead of 1/3, exit 2, outside Julia and Dagger. Serialization flags 3 passed all three trials, exit 0. This establishes a native-stack correctness limitation for this specific control on gfx1036; it does not identify the precise mechanism or validate failed Dagger timings. The exact outputs/source/binary SHA are preserved in native-hip-ordering-diagnostic.json. CUDA Distributed fresh local-probe pairs completed 35/37 leaves in both orders without confirmed flags. Intel Distributed baseline native guard crashed in the SYCL kernel-name path, current 37 leaves passed, and no new paired ratios are accepted.

## Completed supplemental executions

| Supplement | Execution | Outcome |
| --- | --- | --- |
| Native HIP | build | Passed, exit 0 |
| Native HIP | native | Failed, exit 2 |
| Native HIP | serialization-control | Passed, exit 0 |
| Intel GPU stencils (original setup failure) | intel-stencil-plain-baseline | Failed, exit 1 |
| Intel GPU stencils (original setup failure) | intel-stencil-plain-current | Failed, exit 1 |
| Intel GPU stencils (original setup failure) | intel-stencil-distributed-baseline | Failed, exit 1 |
| Intel GPU stencils (original setup failure) | intel-stencil-distributed-current | Failed, exit 1 |
| Intel GPU stencils (original setup failure) | intel-stencil-mpi-baseline | Failed, exit 1 |
| Intel GPU stencils (original setup failure) | intel-stencil-mpi-current | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-plain-baseline-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-plain-current-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-distributed-baseline-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-distributed-current-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-mpi-baseline-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (first edit was a no-op) | intel-stencil-mpi-current-helper-fixed | Failed, exit 1 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-plain-baseline-exact-helper | Failed, exit 1 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-plain-current-exact-helper | Failed, exit 1 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-distributed-baseline-exact-helper | Failed, exit -15 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-distributed-current-exact-helper | Failed, exit-9 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-mpi-baseline-exact-helper | Failed, exit 124 |
| Intel GPU stencils (exact wrapper verified) | intel-stencil-mpi-current-exact-helper | Failed, exit 1 |
| Intel benchmark wrappers (Distributed resource shards) | intel-stencil-distributed-baseline-benchmark-wrapper | Passed, exit 0 |
| Intel benchmark wrappers (Distributed resource shards) | intel-stencil-distributed-current-benchmark-wrapper | Passed, exit 0 |
| CPU no-op synchronization RPC control | cpu_sync_rpc_control | Passed, exit 0 |
| MPI CI layouts | mpi-ci-2 ranks-2-1 | Passed, exit 0; 12 assertions / rank |
| MPI CI layouts | mpi-ci-4ranks-2-1 | Passed, exit 0; 12 assertions / rank |
| Independent Intel SGEMM | intel-native-sgemm-cold-concurrent | Passed, exit 0; 64 validated GEMMs across 2 ranks; Dagger not loaded |
| Independent Intel SGEMM | intel-native-sgemm-serial | Passed, exit 0; 64 validated GEMMs across 2 ranks; Dagger not loaded |
| Independent Intel SGEMM | intel-native-sgemm-warm-concurrent | Passed, exit 0; 80 validated GEMMs across 2 ranks; Dagger not loaded |
| AMD async profile | amd-distributed-epoch-profile | Passed, exit 0 |
| CPU Distributed epoch profile | cpu-distributed-epoch-profile-baseline | Passed, exit 0 |
| CPU Distributed epoch profile | cpu-distributed-epoch-profile-current | Passed, exit 0 |
| Intel driver allocation profile | skipped | Skipped: No confirmed Intel Distributed driver allocation flags |
| CUDA broadcast profile | skipped | Skipped: No independently confirmed CUDA Distributed broadcast timing flag; retain initial/reversed broad observations. |
| Conditional fixed SVD profile | skipped | Skipped: No independently confirmed tile 128 large Distributed SVD timing flag; preserve initial/reverse observations. |

The verified Intel shared-stencil supplement passed 66 assertions and errored once in each plain revision: an unguarded 3D tuple-neighborhood requires 2,760 kernel argument bytes, exceeding the 2,048-byte oneAPI compiler limit. All16 exact benchmark wrappers passed in those plain runs. `skip_highdim=true` only skips the issue-669 3D/4D loop, not this tuple case. The full Distributed baseline was resource-stopped; the integrated driver was kernel OOM-killed. Original and first no-op helper-edit attempts failed setup before any assertions. Later wrapper-only shards establish only their own 16 assertions. Integrated MPI also finished with 66 passes / 1 error on each rank and all 16 benchmark wrappers passed; baselineMPI timed out after IPC-close ambiguity. Interleaved MPI summaries and both rank exception totals are preserved in the raw log. The shared3D error remains failed coverage.

Native HIP uses a separate C++ stream-ordering program with three trials per mode, outside Julia and Dagger. The native run returned 0/0 instead of 1/3 on its first trial and exited2; serialization flags 3 passed all three trials. These are separate correctness controls, not performance comparisons. The exact native mechanism remains unresolved. The independent Intel controls exercise full-size128/256 SGEMM through oneAPI and MPI with shared public contexts; they do not reproduce every Dagger scheduling, IPC, or execution-task detail. A passing control does not resolve the final Intel MPI SYCL crash. See [supplement-execution-audit.json](supplement-execution-audit.json) for exact outputs, assertion summaries, device/numerical measurement provenance and finalizer counts.

### Coverage-preserving resource shards

| Execution | Outcome | Timed leaves |
| --- | --- | --- | --- |
| amdgpu-distributed-baseline-resource-shard-array | Passed, exit 0 | 14 |
| amdgpu-distributed-current-resource-shard-array | Passed, exit 0 | 14 |
| amdgpu-distributed-baseline-resource-shard-linalg | Passed, exit 0 | 2 |
| amdgpu-distributed-current-resource-shard-linalg | Passed, exit 0 | 4 |
| amdgpu-distributed-baseline-resource-shard-sparse | Failed, exit -15 | — |
| amdgpu-distributed-current-resource-shard-sparse | Passed, exit 0 | 3 |
| amdgpu-distributed-baseline-resource-shard-stencil | Passed, exit 0 | 16 |
| amdgpu-distributed-current-resource-shard-stencil | Passed, exit 0 | 16 |
| amdgpu-distributed-current-resource-shard-stencil-r2 | Passed, exit 0 | 16 |
| amdgpu-distributed-baseline-resource-shard-stencil-r2 | Passed, exit 0 | 16 |
| cpu-distributed-baseline-large-linalg-b64-without-svd | Passed, exit 0 | 7 |
| cpu-distributed-current-large-linalg-b64-without-svd | Passed, exit 0 | 7 |
| amdgpu-distributed-baseline-resource-leaf-sparse-spmv | Failed, exit -15 | — |
| amdgpu-distributed-current-resource-leaf-sparse-spmv | Failed, exit -15 | — |
| amdgpu-distributed-baseline-resource-leaf-sparse-cg | Failed, exit -15 | — |
| amdgpu-distributed-current-resource-leaf-sparse-cg | Failed, exit -15 | — |
| amdgpu-distributed-baseline-resource-leaf-sparse-spgemm | Passed, exit 0 | 1 |
| amdgpu-distributed-current-resource-leaf-sparse-spgemm | Passed, exit 0 | 1 |
| amdgpu-distributed-baseline-disk-reclaimed-sparse-spmv | Passed, exit 0 | 1 |
| amdgpu-distributed-current-disk-reclaimed-sparse-spmv | Passed, exit 0 | 1 |
| amdgpu-distributed-baseline-disk-reclaimed-sparse-cg | Passed, exit 0 | 1 |
| amdgpu-distributed-current-disk-reclaimed-sparse-cg | Failed, exit -15 | — |

Shards preserve the original fixture sizes, backend modes, process/thread layout, warmups and sample protocol while starting each suite in a fresh process. Original resource-stopped runs remain incomplete; recovered suites do not erase failed SVD or native numerical guards. Pairwise findings and reversed-order confirmation are retained in the performance summary. The last GPU sparse reassessment followed byte-identical relocation of 24 completed allocation profiles (1071MiB) from RAM-backed `/tmp` to durable disk; it retains the original 512 / four-block fixture and topology. Resource-monitor polling was tightened from 2 seconds to 100 ms after an observed Intel kernel OOM, with the same 768 MiB stop threshold. SpMV completed on both revisions with seven samples and no initial flags; baseline CG completed but integrated CG was resource-stopped at 628 MiB available, so CG has no accepted pairwise ratio. Earlier SpGEMM completed on both revisions. See [gpu-sparse-disk-recovery-conclusions.json](gpu-sparse-disk-recovery-conclusions.json), [profile-storage-relocation.json](profile-storage-relocation.json) and [resource-monitor-polling-adjustment.json](resource-monitor-polling-adjustment.json).

### Epoch profile measurements

| Context/revision | Mode | Median ms | Minimum driver allocations | Minimum GC-accounted bytes |
| --- | --- | --- | --- | --- |
| amd-distributed-epoch-profile | async/8 regions/hier=false | 107.440 | 23589 | 1181840 |
| amd-distributed-epoch-profile | sync/8 regions/hier=false | 80.799 | 36927 | 1833408 |
| amd-distributed-epoch-profile | async/8 regions/hier=true | 106.227 | 28101 | 1437112 |
| amd-distributed-epoch-profile | sync/8 regions/hier=true | 80.713 | 41841 | 2137688 |
| cpu-distributed-epoch-profile-baseline | sync/8 regions/hier=false | 7.665 | 24665 | 1287520 |
| cpu-distributed-epoch-profile-baseline | sync/8 regions/hier=true | 8.931 | 28526 | 1558456 |
| cpu-distributed-epoch-profile-current | async/8 regions/hier=false | 6.360 | 19360 | 1049472 |
| cpu-distributed-epoch-profile-current | sync/8 regions/hier=false | 9.001 | 27713 | 1468640 |
| cpu-distributed-epoch-profile-current | async/8 regions/hier=true | 6.134 | 22717 | 1305784 |
| cpu-distributed-epoch-profile-current | sync/8 regions/hier=true | 9.903 | 32183 | 1757736 |

[epoch-profile-analysis.json](epoch-profile-analysis.json) contains CPU sample frames and exclusive nearest-Dagger allocation buckets. CPU/Intel/CUDA profiles sample the driver; AMD samples all processes and records bounded-buffer fullness. Timer-control RPC overhead is confined to diagnostics. CPU samples do not directly measure GPU execution latency. Allocation profiles are separate instrumented calls; their payload-byte sums must not replace GC-accounted minima from seven ordinary samples. Paired CPU attribution is also preserved in [paired-epoch-profile-attribution.json](paired-epoch-profile-attribution.json); the isolated CPU no-op RPC control did not show a repeatable timing improvement. Additional conclusions from the actual profiles and native diagnostics are recorded in [diagnostic-conclusions.md](diagnostic-conclusions.md).

## Reproduction

Live runner directory: `/tmp/dagger-dev-validation`. Durable local archive: `benchmark_results/dev-integration-20261009`. [artifact-sha256.json](artifact-sha256.json) covers all saved artifacts; [reproduction/HANDOFF.md](reproduction/HANDOFF.md) records restart/controller ownership. Archive source revisions and environment manifests are included. Run finalize_report.py and then snapshot_artifacts.py to regenerate the completed report and checksums from the saved records. The exact loaded oneAPI 2.9.2 JVTbl source is archived with its SHA; the older mislabelled sibling archive is retained only as historical evidence. Metadata distinguishes historical harness hashes from final script hashes. Preserve failed/partial historical evidence and never compare it as a successful run.
