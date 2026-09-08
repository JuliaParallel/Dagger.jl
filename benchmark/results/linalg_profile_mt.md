| Key | Problem | Dagger | Host | Allocs / bytes | Tasks | CPU buckets (sample %) | Notes |
|---|---|---|---|---|---|---|---|
| blas1_dot_4tile | n=4096, tiles=4, block=1024 | 476.5 µs | 0.7 µs | 4047 / 135072 | 4 | scheduler=47.3%, other=100.0% | Krylov calls this every iteration |
| blas1_axpy_4tile | n=4096, tiles=4, block=1024 | 1.23 ms | 0.7 µs | 6301 / 283856 | 4 | scheduler=50.5%, datadeps=2.8%, spawn_thunk=2.8%, linalg_wrapper=2.8%, other=100.0% | Krylov calls this every iteration |
| blas1_axpby_4tile | n=4096, tiles=4, block=1024 | 949.2 µs | 2.4 µs | 6292 / 283664 | 4 | scheduler=46.1%, datadeps=2.8%, spawn_thunk=2.8%, linalg_wrapper=2.8%, other=100.0% | Krylov calls this every iteration |
| blas1_rmul_4tile | n=4096, tiles=4, block=1024 | 775.3 µs | 0.9 µs | 5827 / 259488 | 4 | scheduler=47.4%, datadeps=2.6%, spawn_thunk=3.5%, linalg_wrapper=2.6%, other=100.0% | Krylov calls this every iteration |
| blas1_norm_4tile | n=4096, tiles=4, block=1024 | 971.1 µs | 1.2 µs | 4228 / 144896 | 4 | scheduler=46.4%, spawn_thunk=3.6%, linalg_wrapper=3.6%, other=100.0% | Krylov calls this every iteration |
| blas1_copyto_4tile | n=4096, tiles=4, block=1024 | 983.3 µs | 0.7 µs | 6599 / 294592 | 4 | scheduler=50.0%, datadeps=2.6%, spawn_thunk=2.6%, linalg_wrapper=2.6%, other=100.0% | Krylov calls this every iteration |
| blas1_fill_4tile | n=4096, tiles=4, block=1024 | 1.19 ms | 0.2 µs | 5786 / 258432 | 4 | scheduler=50.5%, datadeps=3.9%, spawn_thunk=3.9%, linalg_wrapper=3.9%, other=100.0% | Krylov calls this every iteration |
| blas1_dot_16tile | n=4096, tiles=16, block=256 | 1.15 ms | 0.7 µs | 16204 / 543200 | 16 | scheduler=51.0%, spawn_thunk=2.0%, linalg_wrapper=2.0%, other=100.0% | Krylov calls this every iteration |
| blas1_axpy_16tile | n=4096, tiles=16, block=256 | 3.0 ms | 0.8 µs | 23214 / 1047312 | 16 | scheduler=48.0%, datadeps=3.1%, spawn_thunk=3.1%, linalg_wrapper=3.1%, other=100.0% | Krylov calls this every iteration |
| blas1_axpby_16tile | n=4096, tiles=16, block=256 | 3.02 ms | 2.4 µs | 24091 / 1077488 | 16 | scheduler=48.8%, datadeps=2.1%, spawn_thunk=2.5%, linalg_wrapper=2.1%, other=100.0% | Krylov calls this every iteration |
| blas1_rmul_16tile | n=4096, tiles=16, block=256 | 2.63 ms | 0.3 µs | 21499 / 950288 | 16 | scheduler=48.1%, datadeps=3.1%, spawn_thunk=3.1%, linalg_wrapper=3.1%, other=100.0% | Krylov calls this every iteration |
| blas1_norm_16tile | n=4096, tiles=16, block=256 | 1.67 ms | 1.2 µs | 16078 / 541632 | 16 | scheduler=51.5%, spawn_thunk=3.0%, linalg_wrapper=3.0%, other=100.0% | Krylov calls this every iteration |
| blas1_copyto_16tile | n=4096, tiles=16, block=256 | 2.97 ms | 0.6 µs | 24550 / 1088688 | 16 | scheduler=47.8%, datadeps=3.2%, spawn_thunk=3.2%, linalg_wrapper=3.2%, other=100.0% | Krylov calls this every iteration |
| blas1_fill_16tile | n=4096, tiles=16, block=256 | 2.32 ms | 0.2 µs | 21412 / 942800 | 16 | scheduler=48.8%, datadeps=3.7%, spawn_thunk=3.7%, linalg_wrapper=3.7%, other=100.0% | Krylov calls this every iteration |
| sparse_spmv | n=160000, nnz=479998, tile=20000, chunks=64, nnz_tiles=22 | 7.26 ms | 538.6 µs | 77178 / 3064936 | 64 | scheduler=47.0%, datadeps=2.5%, spawn_thunk=3.2%, linalg_wrapper=2.5%, other=100.0% | gemv_dagger! spawns every (row,col) tile pair, including structural zeros |
| sparse_spmv_krylov4 | n=4096, nnz=20224, tile=1024, chunks=16, nnz_tiles=10 | 3.69 ms | 17.4 µs | 23243 / 1026800 | 16 | scheduler=47.7%, datadeps=3.2%, spawn_thunk=3.2%, linalg_wrapper=3.2%, other=100.0% | gemv_dagger! spawns every (row,col) tile pair, including structural zeros |
| sparse_spmv_krylov16 | n=4096, nnz=20224, tile=256, chunks=256, nnz_tiles=46 | 22.88 ms | 17.7 µs | 290948 / 11227640 | 256 | scheduler=47.1%, datadeps=2.8%, spawn_thunk=2.8%, linalg_wrapper=2.8%, other=100.0% | gemv_dagger! spawns every (row,col) tile pair, including structural zeros |
| dense_gemm | n=4096, tile=512×512, C←A*B | 331.98 ms | 240.95 ms | 439961 / 149356976 | 576 | scheduler=47.2%, spawn_thunk=23.2%, linalg_wrapper=23.2%, blas=23.2%, other=100.0% | Dagger BLAS=1; host BLAS=16; closest published row (0.80×) |
| krylov_cg_oneiter | 2-D Laplacian 64×64, tile=1024×1024 | 11.95 ms | — | 57591 / 2393792 | 44 | scheduler=48.1%, datadeps=1.5%, spawn_thunk=2.7%, linalg_wrapper=2.5%, other=100.0% | 1 SpMV + 3 dots + 2 axpy + 1 axpby + 1 norm (not Krylov.jl itself) |
| krylov_cg_short | 2-D Laplacian 64×64, itmax=8, tile=1024×1024 | 101.26 ms | — | 478493 / 20254256 | 336 | scheduler=48.6%, datadeps=2.1%, spawn_thunk=2.8%, linalg_wrapper=2.8%, krylov=2.8%, other=100.0% | short itmax; published full solve is 196 iters / 3.37 s |
| krylov_gmres_short | 2-D Laplacian 64×64, itmax=8, memory=50 | 245.5 ms | — | 1069223 / 44195088 | 748 | scheduler=48.7%, datadeps=2.3%, spawn_thunk=2.8%, linalg_wrapper=2.3%, krylov=2.8%, other=100.0% | short itmax; published full solve is 192 iters / 77.4 s |
| pc_jacobi_apply | 2-D Laplacian 64×64, tile=1024×1024 | 1.8 ms | — | 8240 / 382752 | 4 | scheduler=47.9%, datadeps=3.1%, spawn_thunk=3.1%, linalg_wrapper=3.1%, other=100.0% | setup excluded; one mul! of JacobiPreconditioner |
| pc_blockjacobi_apply | 2-D Laplacian 64×64, tile=1024×1024 | 1.92 ms | — | 8038 / 431504 | 4 | scheduler=48.0%, datadeps=2.0%, spawn_thunk=2.0%, linalg_wrapper=2.0%, other=100.0% | setup=0.563s excluded from apply |
| assembly | 2-D Laplacian COO 200×200, nnz=199200, tile=2500×2500 | 112.42 ms | 30.9 ms | 740065 / 58972904 | 512 | scheduler=49.2%, spawn_thunk=2.8%, linalg_wrapper=1.5%, sparse_compute=3.2%, other=100.0% | host baseline is sparse then distribute |
| sparse_chol_apply | 2-D Laplacian 80×80, tile=1280×1280 | 1.29 ms | 405.1 µs | 6878 / 686040 | 6 | scheduler=57.1%, spawn_thunk=7.1%, sparse_compute=7.1%, blas=7.1%, other=100.0% | setup=3.572s (gather+CHOLMOD) excluded from apply |
| sparse_chol_factor_solve | 2-D Laplacian 80×80, tile=1280×1280 | 2.258 s | — | 286742796 / 9845417008 | 32 | scheduler=48.8%, spawn_thunk=36.0%, other=100.0% | includes gather; published combined time 2.42 s |
| projected | 1-D Laplacian n=2048, tile=256 | 16.0 ms | — | 132711 / 5242584 | 104 | scheduler=47.6%, datadeps=2.0%, spawn_thunk=2.5%, linalg_wrapper=2.0%, other=100.0% | 3 SpMV-class applies (P A P); correctness-adjacent |
| dense_lu | n=2048, tile=256×256 | 167.33 ms | 54.26 ms | 894958 / 85624664 | 752 | scheduler=48.2%, datadeps=2.2%, spawn_thunk=4.9%, linalg_wrapper=2.3%, blas=2.3%, other=100.0% | tiled getrf vs host OpenBLAS=16 |

### Top inclusive frames (per kernel)

#### blas1_dot_4tile
samples=463
- 873  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 432  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 432  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 259  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 241  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 230  `wait @ ./task.jl:1228`
- 229  `poptask @ ./task.jl:1216`
- 228  `#wait#406 @ ./condition.jl:141`
- 228  `wait @ ./condition.jl:136`
- 218  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 214  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 212  `wait @ ./lock.jl:623`
- 212  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 212  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 21  ` @ :-1`
- 19  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 19  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 16  `take! @ ./channels.jl:526`
- 16  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
- 16  `take_buffered @ ./channels.jl:532`
- 15  `_jl_invoke @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gf.c:4006`
- 15  `ijl_apply_generic @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gf.c:4214`
- 13  `jl_compile_method_internal @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gf.c:3528`
- 13  `jl_compile_codeinst_impl @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jitlayers.cpp:824`
log category time (s): Dict("compute" => 0.088245612, "schedule" => 0.000651132, "add_thunk" => 0.504526558, "fire" => 0.038595354, "proc_run_wait" => 0.022357389999999998, "move" => 0.000223019, "enqueue" => 0.00010615, "storage_safe_scan" => 6.585e-5, "finish" => 3.3138e-5, "proc_steal_local" => 2.5589000000000002e-5, "proc_run_fetch" => 2.1475e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 4, "move" => 12, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 2, "proc_run_fetch" => 4)
sampled alloc types: Dict{String, Any}[Dict("bytes" => 1056, "count" => 33, "type" => "Dagger.ThreadProc"), Dict("bytes" => 928, "count" => 29, "type" => "Dagger.Sch.var\"#start_processor_runner!##6#start_processor_runner!##7\"{UInt32, Dagger.ThreadProc}"), Dict("bytes" => 864, "count" => 3, "type" => "TimespanLogging.Event{:finish}"), Dict("bytes" => 800, "count" => 25, "type" => "Dagger.LockedObject{DataStructures.PriorityQueue{Dagger.Sch.TaskSpec, UInt32, Base.Order.ForwardOrdering}}"), Dict("bytes" => 224, "count" => 14, "type" => "OSProc"), Dict("bytes" => 128, "count" => 8, "type" => "Float64"), Dict("bytes" => 128, "count" => 2, "type" => "MemPool.RefState"), Dict("bytes" => 96, "count" => 3, "type" => "Vector{UInt64}"), Dict("bytes" => 96, "count" => 6, "type" => "Core.Box"), Dict("bytes" => 96, "count" => 2, "type" => "Memory{Int64}"), Dict("bytes" => 80, "count" => 1, "type" => "Memory{UInt8}"), Dict("bytes" => 80, "count" => 1, "type" => "Dagger.Signature")]

#### blas1_axpy_4tile
samples=107
- 185  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 83  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 83  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 61  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 55  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 53  `wait @ ./condition.jl:136`
- 52  `#wait#406 @ ./condition.jl:141`
- 52  `poptask @ ./task.jl:1216`
- 52  `wait @ ./task.jl:1228`
- 51  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 49  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 49  `wait @ ./lock.jl:623`
- 49  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 34  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 26  ` @ :-1`
- 15  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 13  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 13  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 12  `trypoptask @ ./task.jl:1208`
- 8  `multiq_deletemin @ ./partr.jl:202`
- 4  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 4  `notify @ ./condition.jl:159`
- 4  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 4  `trylock @ ./locks-mt.jl:53`
log category time (s): Dict("compute" => 0.00018510599999999999, "schedule" => 0.01198172, "add_thunk" => 0.012200087, "fire" => 0.000236497, "proc_run_wait" => 0.017579819, "move" => 0.000249579, "enqueue" => 2.4179e-5, "storage_safe_scan" => 1.5063e-5, "finish" => 5.4294999999999994e-5, "proc_steal_local" => 7.1423e-5, "proc_run_fetch" => 3.2195e-5, "datadeps_execute" => 0.012269963, "datadeps_copy_skip" => 5.7756999999999996e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 8, "move" => 24, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 6, "proc_run_fetch" => 8, "datadeps_execute" => 4, "datadeps_copy_skip" => 8)
sampled alloc types: Dict{String, Any}[Dict("bytes" => 1728, "count" => 6, "type" => "TimespanLogging.Event{:finish}"), Dict("bytes" => 1472, "count" => 46, "type" => "Dagger.ThreadProc"), Dict("bytes" => 1280, "count" => 40, "type" => "Dagger.LockedObject{DataStructures.PriorityQueue{Dagger.Sch.TaskSpec, UInt32, Base.Order.ForwardOrdering}}"), Dict("bytes" => 1216, "count" => 38, "type" => "Dagger.Sch.var\"#start_processor_runner!##6#start_processor_runner!##7\"{UInt32, Dagger.ThreadProc}"), Dict("bytes" => 928, "count" => 16, "type" => "ExactScope"), Dict("bytes" => 864, "count" => 3, "type" => "TimespanLogging.Event{:start}"), Dict("bytes" => 816, "count" => 2, "type" => "Memory{Any}"), Dict("bytes" => 768, "count" => 1, "type" => "NTuple{16, ExactScope}"), Dict("bytes" => 544, "count" => 4, "type" => "Memory{Dagger.AbstractScope}"), Dict("bytes" => 288, "count" => 2, "type" => "Memory{Dagger.AbstractAliasing}"), Dict("bytes" => 280, "count" => 7, "type" => "Memory{Int64}"), Dict("bytes" => 272, "count" => 1, "type" => "Memory{Dagger.ThreadProc}")]

#### blas1_axpby_4tile
samples=141
- 256  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 115  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 115  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 78  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 68  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 68  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 66  `wait @ ./condition.jl:136`
- 65  `#wait#406 @ ./condition.jl:141`
- 65  `poptask @ ./task.jl:1216`
- 65  `wait @ ./task.jl:1228`
- 63  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 61  `wait @ ./lock.jl:623`
- 61  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 47  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 32  ` @ :-1`
- 13  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 13  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 12  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 12  `trypoptask @ ./task.jl:1208`
- 5  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 5  `profile_blas1 @ /home/ubuntu/work/Dagger.jl/benchmark/suites/linalg_profile.jl:410`
- 5  `jl_repl_entrypoint @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jlapi.c:1153`
- 5  `jfptr__start_65616.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 5  `jl_toplevel_eval_flex @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/toplevel.c:1035`
log category time (s): Dict("compute" => 0.000199575, "schedule" => 0.00037703700000000004, "add_thunk" => 0.000538637, "fire" => 0.000204389, "proc_run_wait" => 0.0009445189999999999, "move" => 0.000251946, "enqueue" => 3.6422e-5, "storage_safe_scan" => 4.1158e-5, "finish" => 1.8267e-5, "proc_steal_local" => 0.000170357, "proc_run_fetch" => 4.500799999999999e-5, "datadeps_execute" => 0.0006293420000000001, "datadeps_copy_skip" => 5.4701000000000004e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 9, "move" => 28, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 8, "proc_run_fetch" => 9, "datadeps_execute" => 4, "datadeps_copy_skip" => 8)

#### blas1_rmul_4tile
samples=114
- 207  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 96  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 96  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 64  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 57  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 55  `#wait#406 @ ./condition.jl:141`
- 55  `wait @ ./condition.jl:136`
- 55  `poptask @ ./task.jl:1216`
- 55  `wait @ ./task.jl:1228`
- 54  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 52  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 51  `wait @ ./lock.jl:623`
- 51  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 42  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 23  ` @ :-1`
- 13  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 13  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 13  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 12  `trypoptask @ ./task.jl:1208`
- 5  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 5  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 4  `trylock @ ./locks-mt.jl:53`
- 4  `multiq_deletemin @ ./partr.jl:202`
- 4  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.000215212, "schedule" => 0.00042833, "add_thunk" => 0.00059646, "fire" => 0.000211989, "proc_run_wait" => 0.001275877, "move" => 0.00019507399999999994, "enqueue" => 3.9322e-5, "storage_safe_scan" => 3.6393e-5, "finish" => 3.436e-5, "proc_steal_local" => 0.000152772, "proc_run_fetch" => 4.3771000000000005e-5, "datadeps_execute" => 0.0006678359999999999, "datadeps_copy_skip" => 2.8550999999999998e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 10, "move" => 16, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 8, "proc_run_fetch" => 9, "datadeps_execute" => 4, "datadeps_copy_skip" => 4)

#### blas1_norm_4tile
samples=28
- 47  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 18  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 18  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 15  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 14  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 13  `#wait#406 @ ./condition.jl:141`
- 13  `wait @ ./condition.jl:136`
- 13  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 13  `poptask @ ./task.jl:1216`
- 13  `wait @ ./task.jl:1228`
- 12  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 12  `wait @ ./lock.jl:623`
- 12  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 8  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 8  `trypoptask @ ./task.jl:1208`
- 8  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 8  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 4  `trylock @ ./locks-mt.jl:53`
- 4  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 4  `multiq_deletemin @ ./partr.jl:202`
- 3  ` @ :-1`
- 1  `macro expansion @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1076`
- 1  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 1  `< @ ./int.jl:519`
log category time (s): Dict("compute" => 0.00019791499999999998, "schedule" => 0.00036685799999999996, "add_thunk" => 0.0005952959999999999, "fire" => 0.00023612200000000001, "proc_run_wait" => 0.000709782, "move" => 0.000113179, "enqueue" => 8.196800000000001e-5, "storage_safe_scan" => 2.1483e-5, "finish" => 5.0806e-5, "proc_steal_local" => 7.2327e-5, "proc_run_fetch" => 0.000159643)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 9, "move" => 16, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 7, "proc_run_fetch" => 9)

#### blas1_copyto_4tile
samples=156
- 278  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 132  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 132  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 89  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 81  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 80  `wait @ ./condition.jl:136`
- 79  `poptask @ ./task.jl:1216`
- 79  `wait @ ./task.jl:1228`
- 78  `#wait#406 @ ./condition.jl:141`
- 75  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 74  `wait @ ./lock.jl:623`
- 74  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 71  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 61  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 31  ` @ :-1`
- 15  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 15  `trypoptask @ ./task.jl:1208`
- 15  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 15  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 9  `multiq_deletemin @ ./partr.jl:202`
- 7  `trylock @ ./locks-mt.jl:53`
- 6  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 6  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 6  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.000151887, "schedule" => 0.00040264900000000004, "add_thunk" => 0.0005537070000000001, "fire" => 0.00015546, "proc_run_wait" => 0.002040616, "move" => 0.0004536320000000001, "enqueue" => 3.1201e-5, "storage_safe_scan" => 2.2839e-5, "finish" => 1.7224e-5, "proc_steal_local" => 7.9311e-5, "proc_run_fetch" => 3.0384e-5, "datadeps_execute" => 0.000611566, "datadeps_copy_skip" => 0.00020543099999999998)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 9, "move" => 28, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 8, "proc_run_fetch" => 9, "datadeps_execute" => 4, "datadeps_copy_skip" => 8)

#### blas1_fill_4tile
samples=103
- 180  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 84  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 84  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 61  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 53  `#wait#406 @ ./condition.jl:141`
- 53  `wait @ ./condition.jl:136`
- 53  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 53  `poptask @ ./task.jl:1216`
- 53  `wait @ ./task.jl:1228`
- 50  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 50  `wait @ ./lock.jl:623`
- 50  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 46  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 38  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 32  ` @ :-1`
- 11  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 11  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 11  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 10  `trypoptask @ ./task.jl:1208`
- 6  `multiq_deletemin @ ./partr.jl:202`
- 5  `#with_options##0 @ /home/ubuntu/work/Dagger.jl/src/options.jl:261`
- 5  `trylock @ ./locks-mt.jl:53`
- 5  `with_options @ /home/ubuntu/work/Dagger.jl/src/options.jl:264`
- 5  `#with_options#102 @ /home/ubuntu/work/Dagger.jl/src/options.jl:264`
log category time (s): Dict("compute" => 0.000169907, "schedule" => 0.00046554500000000004, "add_thunk" => 0.000644861, "fire" => 0.000152946, "proc_run_wait" => 0.001315448, "move" => 0.000189766, "enqueue" => 2.5983e-5, "storage_safe_scan" => 1.5154e-5, "finish" => 1.9747e-5, "proc_steal_local" => 4.9111e-5, "proc_run_fetch" => 6.261e-5, "datadeps_execute" => 0.000708168, "datadeps_copy_skip" => 3.3429e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 7, "move" => 16, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 4, "proc_run_fetch" => 7, "datadeps_execute" => 4, "datadeps_copy_skip" => 4)

#### blas1_dot_16tile
samples=49
- 77  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 32  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 32  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 29  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 26  `#wait#406 @ ./condition.jl:141`
- 26  `wait @ ./condition.jl:136`
- 26  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 26  `poptask @ ./task.jl:1216`
- 26  `wait @ ./task.jl:1228`
- 24  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 24  `wait @ ./lock.jl:623`
- 24  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 22  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 13  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 13  `trypoptask @ ./task.jl:1208`
- 13  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 13  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 10  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 5  `trylock @ ./locks-mt.jl:53`
- 5  `multiq_deletemin @ ./partr.jl:202`
- 4  ` @ :-1`
- 3  `multiq_deletemin @ ./partr.jl:199`
- 2  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 2  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
log category time (s): Dict("compute" => 0.0018455479999999998, "schedule" => 0.011651396000000001, "add_thunk" => 0.012431244000000004, "fire" => 0.000669354, "proc_run_wait" => 0.008539668000000002, "move" => 0.003398756, "enqueue" => 0.000317068, "storage_safe_scan" => 0.0006258860000000001, "finish" => 0.00035625100000000003, "proc_steal_local" => 0.017483857000000002, "proc_run_fetch" => 0.01176309)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 48, "move" => 48, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 29, "proc_run_fetch" => 53)
sampled alloc types: Dict{String, Any}[Dict("bytes" => 24480, "count" => 85, "type" => "TimespanLogging.Event{:finish}"), Dict("bytes" => 19008, "count" => 66, "type" => "TimespanLogging.Event{:start}"), Dict("bytes" => 7040, "count" => 220, "type" => "Dagger.ThreadProc"), Dict("bytes" => 5344, "count" => 167, "type" => "Dagger.Sch.var\"#start_processor_runner!##6#start_processor_runner!##7\"{UInt32, Dagger.ThreadProc}"), Dict("bytes" => 4888, "count" => 153, "type" => "Dagger.LockedObject{DataStructures.PriorityQueue{Dagger.Sch.TaskSpec, UInt32, Base.Order.ForwardOrdering}}"), Dict("bytes" => 3200, "count" => 40, "type" => "Dict{UInt64, Vector{Base.StackTraces.StackFrame}}"), Dict("bytes" => 2304, "count" => 48, "type" => "@NamedTuple{uid::UInt64, worker::Int64, processor::Dagger.ThreadProc}"), Dict("bytes" => 2144, "count" => 67, "type" => "Vector{UInt64}"), Dict("bytes" => 1728, "count" => 36, "type" => "Memory{Int64}"), Dict("bytes" => 1376, "count" => 43, "type" => "Vector{Int64}"), Dict("bytes" => 1072, "count" => 67, "type" => "Core.Box"), Dict("bytes" => 992, "count" => 31, "type" => "@NamedTuple{timestamp::UInt64, category::Symbol, kind::Symbol}")]

#### blas1_axpy_16tile
samples=225
- 395  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 175  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 175  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 126  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 112  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 111  `#wait#406 @ ./condition.jl:141`
- 111  `wait @ ./task.jl:1228`
- 111  `wait @ ./condition.jl:136`
- 110  `poptask @ ./task.jl:1216`
- 106  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 104  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 103  `wait @ ./lock.jl:623`
- 103  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 69  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 55  ` @ :-1`
- 37  `trypoptask @ ./task.jl:1208`
- 37  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 37  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 37  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 18  `multiq_deletemin @ ./partr.jl:202`
- 12  `trylock @ ./locks-mt.jl:53`
- 8  `take! @ ./channels.jl:526`
- 8  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 8  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
log category time (s): Dict("compute" => 0.00100359, "schedule" => 0.002003938, "add_thunk" => 0.0026922760000000004, "fire" => 0.000649199, "proc_run_wait" => 0.024671590000000007, "move" => 0.002516898, "enqueue" => 0.0003313, "storage_safe_scan" => 0.0001706, "finish" => 0.000227563, "proc_steal_local" => 0.0029446259999999992, "proc_run_fetch" => 0.0050348089999999995, "datadeps_execute" => 0.003074005, "datadeps_copy_skip" => 0.00039709499999999987)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 103, "move" => 96, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 84, "proc_run_fetch" => 106, "datadeps_execute" => 16, "datadeps_copy_skip" => 32)
sampled alloc types: Dict{String, Any}[Dict("bytes" => 19872, "count" => 69, "type" => "TimespanLogging.Event{:finish}"), Dict("bytes" => 17280, "count" => 60, "type" => "TimespanLogging.Event{:start}"), Dict("bytes" => 7520, "count" => 235, "type" => "Dagger.ThreadProc"), Dict("bytes" => 5424, "count" => 7, "type" => "NTuple{16, ExactScope}"), Dict("bytes" => 5296, "count" => 166, "type" => "Dagger.LockedObject{DataStructures.PriorityQueue{Dagger.Sch.TaskSpec, UInt32, Base.Order.ForwardOrdering}}"), Dict("bytes" => 5184, "count" => 162, "type" => "Dagger.Sch.var\"#start_processor_runner!##6#start_processor_runner!##7\"{UInt32, Dagger.ThreadProc}"), Dict("bytes" => 3856, "count" => 67, "type" => "ExactScope"), Dict("bytes" => 2480, "count" => 31, "type" => "Dict{UInt64, Vector{Base.StackTraces.StackFrame}}"), Dict("bytes" => 2016, "count" => 42, "type" => "@NamedTuple{uid::UInt64, worker::Int64, processor::Dagger.ThreadProc}"), Dict("bytes" => 1704, "count" => 38, "type" => "Memory{Int64}"), Dict("bytes" => 1504, "count" => 47, "type" => "Vector{UInt64}"), Dict("bytes" => 1368, "count" => 3, "type" => "Memory{Any}")]

#### blas1_axpby_16tile
samples=281
- 498  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 226  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 226  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 161  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 143  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 139  `wait @ ./condition.jl:136`
- 138  `#wait#406 @ ./condition.jl:141`
- 138  `wait @ ./task.jl:1228`
- 138  `poptask @ ./task.jl:1216`
- 133  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 132  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 131  `wait @ ./lock.jl:623`
- 131  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 94  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 51  ` @ :-1`
- 32  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 32  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 31  `trypoptask @ ./task.jl:1208`
- 31  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 11  `multiq_deletemin @ ./partr.jl:202`
- 10  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 10  `trylock @ ./locks-mt.jl:53`
- 10  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 8  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.0030439760000000003, "schedule" => 0.001693989, "add_thunk" => 0.0021554880000000005, "fire" => 0.007269846999999999, "proc_run_wait" => 0.010114956, "move" => 0.008750577999999998, "enqueue" => 0.002131996, "storage_safe_scan" => 0.000992604, "finish" => 0.0005094190000000001, "proc_steal_local" => 0.0009706449999999998, "proc_run_fetch" => 0.004737306999999998, "datadeps_execute" => 0.00239397, "datadeps_copy_skip" => 8.0981e-5)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 39, "move" => 112, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 20, "proc_run_fetch" => 40, "datadeps_execute" => 16, "datadeps_copy_skip" => 32)

#### blas1_rmul_16tile
samples=160
- 283  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 128  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 128  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 90  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 80  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 75  `wait @ ./task.jl:1228`
- 75  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 75  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 75  `wait @ ./condition.jl:136`
- 75  `poptask @ ./task.jl:1216`
- 73  `#wait#406 @ ./condition.jl:141`
- 70  `wait @ ./lock.jl:623`
- 70  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 53  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 39  ` @ :-1`
- 21  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 21  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 18  `trypoptask @ ./task.jl:1208`
- 18  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 11  `multiq_deletemin @ ./partr.jl:202`
- 9  `trylock @ ./locks-mt.jl:53`
- 5  `jl_repl_entrypoint @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jlapi.c:1153`
- 5  `jl_toplevel_eval_flex @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/toplevel.c:1035`
- 5  `__libc_start_main @ /lib/x86_64-linux-gnu/libc.so.6:-1`
log category time (s): Dict("compute" => 0.0008103149999999998, "schedule" => 0.0016820569999999996, "add_thunk" => 0.002230922, "fire" => 0.0001616569999999999, "proc_run_wait" => 0.022815501000000002, "move" => 0.002431417, "enqueue" => 0.00025664, "storage_safe_scan" => 0.00016525699999999998, "finish" => 0.00019785599999999997, "proc_steal_local" => 0.0031379369999999995, "proc_run_fetch" => 0.0038960880000000007, "datadeps_execute" => 0.002731342, "datadeps_copy_skip" => 6.3202e-5)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 99, "move" => 64, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 89, "proc_run_fetch" => 105, "datadeps_execute" => 16, "datadeps_copy_skip" => 16)

#### blas1_norm_16tile
samples=33
- 60  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 26  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 26  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 19  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 16  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 16  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 15  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 13  `#wait#406 @ ./condition.jl:141`
- 13  `wait @ ./lock.jl:623`
- 13  `wait @ ./condition.jl:136`
- 13  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 13  `poptask @ ./task.jl:1216`
- 13  `wait @ ./task.jl:1228`
- 10  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 4  `notify @ ./condition.jl:159`
- 4  ` @ :-1`
- 3  `inst_datatype_env @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jltypes.c:1409`
- 2  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 2  `trypoptask @ ./task.jl:1208`
- 2  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 2  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 2  `#notify#407 @ ./condition.jl:159`
- 2  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1796`
- 2  `getproperty @ ./Base_compiler.jl:54`
log category time (s): Dict("compute" => 0.0010772249999999998, "schedule" => 0.0014508400000000001, "add_thunk" => 0.002907699, "fire" => 0.000992137, "proc_run_wait" => 0.023481857000000002, "move" => 0.0018357349999999998, "enqueue" => 0.000331114, "storage_safe_scan" => 0.000318094, "finish" => 0.00045822000000000007, "proc_steal_local" => 0.003919521, "proc_run_fetch" => 0.004070601000000001)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 95, "move" => 64, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 85, "proc_run_fetch" => 99)

#### blas1_copyto_16tile
samples=253
- 462  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 215  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 215  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 144  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 126  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 123  `wait @ ./task.jl:1228`
- 122  `poptask @ ./task.jl:1216`
- 120  `#wait#406 @ ./condition.jl:141`
- 120  `wait @ ./condition.jl:136`
- 119  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 118  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 113  `wait @ ./lock.jl:623`
- 113  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 96  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 61  ` @ :-1`
- 20  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 19  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 19  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 18  `trypoptask @ ./task.jl:1208`
- 12  `multiq_deletemin @ ./partr.jl:202`
- 11  `trylock @ ./locks-mt.jl:53`
- 8  `jl_repl_entrypoint @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jlapi.c:1153`
- 8  `jl_toplevel_eval_flex @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/toplevel.c:1035`
- 8  `__libc_start_main @ /lib/x86_64-linux-gnu/libc.so.6:-1`
log category time (s): Dict("compute" => 0.0007104359999999999, "schedule" => 0.0015071870000000001, "add_thunk" => 0.002324183, "fire" => -0.0006589610000000001, "proc_run_wait" => 0.026127629000000006, "move" => 0.002806355000000001, "enqueue" => 0.000113741, "storage_safe_scan" => 0.00010847799999999999, "finish" => 0.00014314499999999996, "proc_steal_local" => 0.003501626, "proc_run_fetch" => 0.0037942550000000003, "datadeps_execute" => 0.002716221, "datadeps_copy_skip" => 0.0001497639999999999)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 101, "move" => 112, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 84, "proc_run_fetch" => 101, "datadeps_execute" => 16, "datadeps_copy_skip" => 32)

#### blas1_fill_16tile
samples=164
- 277  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 117  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 117  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 97  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 82  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 76  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 75  `wait @ ./task.jl:1228`
- 75  `wait @ ./condition.jl:136`
- 75  `poptask @ ./task.jl:1216`
- 74  `#wait#406 @ ./condition.jl:141`
- 74  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 70  `wait @ ./lock.jl:623`
- 70  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 48  ` @ :-1`
- 41  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 20  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 20  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 19  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 18  `trypoptask @ ./task.jl:1208`
- 13  `check_empty @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:320`
- 13  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:377`
- 9  `multiq_deletemin @ ./partr.jl:202`
- 8  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 8  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
log category time (s): Dict("compute" => 0.000666766, "schedule" => 0.0016237689999999997, "add_thunk" => 0.002349299, "fire" => 0.0006999709999999999, "proc_run_wait" => 0.03838318599999999, "move" => 0.0068579999999999995, "enqueue" => 0.000112366, "storage_safe_scan" => 8.9896e-5, "finish" => 0.000118185, "proc_steal_local" => 0.004837563, "proc_run_fetch" => 0.007938529, "datadeps_execute" => 0.0027567269999999996, "datadeps_copy_skip" => 0.00018480499999999996)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 89, "move" => 64, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 83, "proc_run_fetch" => 89, "datadeps_execute" => 16, "datadeps_copy_skip" => 16)

#### sparse_spmv
samples=317
- 563  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 253  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 253  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 194  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 159  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 150  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 141  `wait @ ./condition.jl:136`
- 136  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 135  `#wait#406 @ ./condition.jl:141`
- 135  `wait @ ./task.jl:1228`
- 135  `poptask @ ./task.jl:1216`
- 129  `wait @ ./lock.jl:623`
- 128  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 103  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 81  ` @ :-1`
- 23  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 23  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 20  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 20  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 18  `trypoptask @ ./task.jl:1208`
- 18  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 13  `jl_f_invokelatest @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:881`
- 12  `take! @ ./channels.jl:526`
- 12  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
log category time (s): Dict("compute" => 0.008530448, "schedule" => 0.012249988000000005, "add_thunk" => 0.010264307, "fire" => 0.0057394920000000006, "proc_run_wait" => 0.1655867950000001, "move" => 0.02513573100000001, "enqueue" => 0.0015723629999999996, "storage_safe_scan" => 0.0013324559999999999, "finish" => 0.0016770169999999994, "proc_steal_local" => 0.04338301299999999, "proc_run_fetch" => 0.05775088199999998, "datadeps_execute" => 0.013914934999999998, "datadeps_copy_skip" => 0.0008280050000000003)
log category count: Dict("compute" => 64, "schedule" => 64, "add_thunk" => 64, "fire" => 64, "proc_run_wait" => 729, "move" => 528, "enqueue" => 64, "storage_safe_scan" => 64, "finish" => 64, "proc_steal_local" => 605, "proc_run_fetch" => 771, "datadeps_execute" => 64, "datadeps_copy_skip" => 80)

#### sparse_spmv_krylov4
samples=222
- 385  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 170  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 170  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 126  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 111  `wait @ ./condition.jl:136`
- 111  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 109  `#wait#406 @ ./condition.jl:141`
- 109  `wait @ ./task.jl:1228`
- 109  `poptask @ ./task.jl:1216`
- 104  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 102  `wait @ ./lock.jl:623`
- 101  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 101  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 66  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 56  ` @ :-1`
- 36  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 36  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 31  `trypoptask @ ./task.jl:1208`
- 31  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 10  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 10  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 9  `take! @ ./channels.jl:526`
- 9  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
- 9  `take_buffered @ ./channels.jl:532`
log category time (s): Dict("compute" => 0.0007046029999999999, "schedule" => 0.002508432, "add_thunk" => 0.002363013, "fire" => 0.0006916599999999999, "proc_run_wait" => 0.061355692999999996, "move" => 0.0029363020000000004, "enqueue" => 0.00018946399999999997, "storage_safe_scan" => 0.00010316999999999998, "finish" => 0.000161521, "proc_steal_local" => 0.0038707900000000007, "proc_run_fetch" => 0.004252580000000001, "datadeps_execute" => 0.002839943, "datadeps_copy_skip" => 0.00022709699999999997)
log category count: Dict("compute" => 16, "schedule" => 16, "add_thunk" => 16, "fire" => 16, "proc_run_wait" => 102, "move" => 136, "enqueue" => 16, "storage_safe_scan" => 16, "finish" => 16, "proc_steal_local" => 87, "proc_run_fetch" => 104, "datadeps_execute" => 16, "datadeps_copy_skip" => 24)
sampled alloc types: Dict{String, Any}[Dict("bytes" => 21888, "count" => 76, "type" => "TimespanLogging.Event{:finish}"), Dict("bytes" => 18720, "count" => 65, "type" => "TimespanLogging.Event{:start}"), Dict("bytes" => 7216, "count" => 226, "type" => "Dagger.ThreadProc"), Dict("bytes" => 5856, "count" => 183, "type" => "Dagger.LockedObject{DataStructures.PriorityQueue{Dagger.Sch.TaskSpec, UInt32, Base.Order.ForwardOrdering}}"), Dict("bytes" => 5696, "count" => 178, "type" => "Dagger.Sch.var\"#start_processor_runner!##6#start_processor_runner!##7\"{UInt32, Dagger.ThreadProc}"), Dict("bytes" => 3984, "count" => 69, "type" => "ExactScope"), Dict("bytes" => 2304, "count" => 3, "type" => "NTuple{16, ExactScope}"), Dict("bytes" => 2176, "count" => 41, "type" => "Memory{Int64}"), Dict("bytes" => 2160, "count" => 27, "type" => "Dict{UInt64, Vector{Base.StackTraces.StackFrame}}"), Dict("bytes" => 1824, "count" => 38, "type" => "@NamedTuple{uid::UInt64, worker::Int64, processor::Dagger.ThreadProc}"), Dict("bytes" => 1792, "count" => 56, "type" => "Vector{UInt64}"), Dict("bytes" => 1696, "count" => 6, "type" => "Memory{Dagger.ArgumentWrapper}")]

#### sparse_spmv_krylov16
samples=784
- 1298  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 536  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 536  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 464  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 395  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 367  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 347  `wait @ ./condition.jl:136`
- 344  `wait @ ./task.jl:1228`
- 342  `poptask @ ./task.jl:1216`
- 338  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 336  `#wait#406 @ ./condition.jl:141`
- 315  `wait @ ./lock.jl:623`
- 312  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 210  ` @ :-1`
- 169  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 136  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 134  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 125  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 123  `trypoptask @ ./task.jl:1208`
- 57  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 57  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 43  `multiq_deletemin @ ./partr.jl:202`
- 38  `trylock @ ./locks-mt.jl:53`
- 32  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.019335664000000006, "schedule" => 0.03432575300000003, "add_thunk" => 0.04721983599999999, "fire" => 0.028311277, "proc_run_wait" => 0.9599716220000021, "move" => 0.110601146, "enqueue" => 0.008605398999999994, "storage_safe_scan" => 0.005129451999999996, "finish" => 0.004984064999999997, "proc_steal_local" => 0.25434167399999985, "proc_run_fetch" => 0.30793225400000035, "datadeps_execute" => 0.07847588100000003, "datadeps_copy_skip" => 0.0020891819999999985)
log category count: Dict("compute" => 256, "schedule" => 256, "add_thunk" => 256, "fire" => 256, "proc_run_wait" => 3794, "move" => 2080, "enqueue" => 256, "storage_safe_scan" => 256, "finish" => 256, "proc_steal_local" => 3261, "proc_run_fetch" => 3953, "datadeps_execute" => 256, "datadeps_copy_skip" => 288)

#### dense_gemm
samples=704
- 1194  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 1151  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 535  ` @ :-1`
- 509  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 509  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 466  `jl_f__apply_iterate @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:868`
- 353  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 330  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 314  `jl_f_invokelatest @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:881`
- 192  `wait @ ./condition.jl:136`
- 191  `wait @ ./task.jl:1228`
- 191  `poptask @ ./task.jl:1216`
- 190  `#wait#406 @ ./condition.jl:141`
- 183  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 183  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 169  `wait @ ./lock.jl:623`
- 169  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 167  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 166  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 160  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:639`
- 154  `execute! @ /home/ubuntu/work/Dagger.jl/src/threadproc.jl:13`
- 154  `DoTaskSpec @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1957`
- 154  `dgemm_64_ @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/bin/../lib/julia/libopenblas64_.so:-1`
- 154  `gemm! @ /cache/build/builder-amdci5-2/julialang/julia-ci/usr/share/julia/stdlib/v1.12/LinearAlgebra/src/blas.jl:1642`
log category time (s): Dict("compute" => 4.430128734, "schedule" => 0.8292392930000001, "add_thunk" => 0.3061213710000002, "fire" => 0.0934432729999999, "proc_run_wait" => 5.25191082299999, "move" => 0.18587428699999975, "enqueue" => 0.017528536999999993, "storage_safe_scan" => 0.05905929399999999, "finish" => 0.030143295000000004, "proc_steal_local" => 0.21831623600000005, "proc_run_fetch" => 0.7568668020000001, "datadeps_execute" => 0.31723872300000006, "datadeps_copy_skip" => 0.0013830779999999996)
log category count: Dict("compute" => 576, "schedule" => 576, "add_thunk" => 576, "fire" => 576, "proc_run_wait" => 3711, "move" => 4544, "enqueue" => 576, "storage_safe_scan" => 576, "finish" => 576, "proc_steal_local" => 2535, "proc_run_fetch" => 4142, "datadeps_execute" => 512, "datadeps_copy_skip" => 192)

#### krylov_cg_oneiter
samples=401
- 705  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 316  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 316  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 232  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 202  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 192  `wait @ ./task.jl:1228`
- 192  `poptask @ ./task.jl:1216`
- 191  `#wait#406 @ ./condition.jl:141`
- 191  `wait @ ./condition.jl:136`
- 189  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 181  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 175  `wait @ ./lock.jl:623`
- 175  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 127  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 85  ` @ :-1`
- 49  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 49  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 46  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 45  `trypoptask @ ./task.jl:1208`
- 21  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 21  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 20  `multiq_deletemin @ ./partr.jl:202`
- 16  `take! @ ./channels.jl:526`
- 16  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
log category time (s): Dict("compute" => 0.002400157, "schedule" => 0.007659318000000002, "add_thunk" => 0.009834470000000001, "fire" => 0.0025988499999999998, "proc_run_wait" => 0.12358606600000008, "move" => 0.008421281000000001, "enqueue" => 0.0005219640000000002, "storage_safe_scan" => 0.000393899, "finish" => 0.000550119, "proc_steal_local" => 0.019303799, "proc_run_fetch" => 0.022051314000000002, "datadeps_execute" => 0.004559433, "datadeps_copy_skip" => 0.0009985219999999995)
log category count: Dict("compute" => 44, "schedule" => 44, "add_thunk" => 44, "fire" => 44, "proc_run_wait" => 440, "move" => 264, "enqueue" => 44, "storage_safe_scan" => 44, "finish" => 44, "proc_steal_local" => 415, "proc_run_fetch" => 448, "datadeps_execute" => 28, "datadeps_copy_skip" => 48)

#### krylov_cg_short
samples=389
- 693  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 316  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 316  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 226  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 196  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 191  `#wait#406 @ ./condition.jl:141`
- 191  `wait @ ./task.jl:1228`
- 191  `wait @ ./condition.jl:136`
- 191  `poptask @ ./task.jl:1216`
- 182  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 179  `wait @ ./lock.jl:623`
- 179  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 177  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 134  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 82  ` @ :-1`
- 46  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 45  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 44  `trypoptask @ ./task.jl:1208`
- 44  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 17  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 17  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 17  `multiq_deletemin @ ./partr.jl:202`
- 16  `trylock @ ./locks-mt.jl:53`
- 12  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.026913341999999996, "schedule" => 0.05835273300000002, "add_thunk" => 0.0794736850000001, "fire" => 0.030801564999999996, "proc_run_wait" => 1.880030990999996, "move" => 0.11665607899999984, "enqueue" => 0.008941877000000003, "storage_safe_scan" => 0.005724404999999997, "finish" => 0.006035928999999998, "proc_steal_local" => 0.31612809799999875, "proc_run_fetch" => 0.38492648699999926, "datadeps_execute" => 0.06188983899999998, "datadeps_copy_skip" => 0.011733134000000006)
log category count: Dict("compute" => 336, "schedule" => 336, "add_thunk" => 336, "fire" => 336, "proc_run_wait" => 5146, "move" => 2100, "enqueue" => 336, "storage_safe_scan" => 336, "finish" => 336, "proc_steal_local" => 4522, "proc_run_fetch" => 5322, "datadeps_execute" => 236, "datadeps_copy_skip" => 404)

#### krylov_gmres_short
samples=704
- 1276  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 592  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 592  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 406  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 354  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 341  `wait @ ./condition.jl:136`
- 339  `wait @ ./task.jl:1228`
- 339  `poptask @ ./task.jl:1216`
- 336  `#wait#406 @ ./condition.jl:141`
- 330  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 325  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 318  `wait @ ./lock.jl:623`
- 318  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 262  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 182  ` @ :-1`
- 61  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 61  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 58  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 57  `trypoptask @ ./task.jl:1208`
- 29  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 29  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 23  `take! @ ./channels.jl:526`
- 23  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
- 23  `take_buffered @ ./channels.jl:532`
log category time (s): Dict("compute" => 0.08233406000000003, "schedule" => 0.16143330900000008, "add_thunk" => 0.26613239699999985, "fire" => 0.07164230900000003, "proc_run_wait" => 4.707079892999971, "move" => 0.22502957299999984, "enqueue" => 0.02392818300000001, "storage_safe_scan" => 0.01829249599999999, "finish" => 0.016709346999999982, "proc_steal_local" => 0.8495425549999961, "proc_run_fetch" => 0.8986083669999967, "datadeps_execute" => 0.10071314800000004, "datadeps_copy_skip" => 0.026005136999999966)
log category count: Dict("compute" => 748, "schedule" => 748, "add_thunk" => 748, "fire" => 748, "proc_run_wait" => 11886, "move" => 3884, "enqueue" => 748, "storage_safe_scan" => 748, "finish" => 748, "proc_steal_local" => 10492, "proc_run_fetch" => 12324, "datadeps_execute" => 344, "datadeps_copy_skip" => 620)

#### pc_jacobi_apply
samples=96
- 180  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 87  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 87  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 56  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 48  `wait @ ./condition.jl:136`
- 48  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 48  `poptask @ ./task.jl:1216`
- 48  `wait @ ./task.jl:1228`
- 47  `#wait#406 @ ./condition.jl:141`
- 45  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 45  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 45  `wait @ ./lock.jl:623`
- 45  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 42  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 22  ` @ :-1`
- 4  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 4  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 3  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 3  `maybe_copy_buffered @ /home/ubuntu/work/Dagger.jl/src/array/copy.jl:8`
- 3  `jl_repl_entrypoint @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jlapi.c:1153`
- 3  `jfptr__start_65616.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 3  `jl_toplevel_eval_flex @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/toplevel.c:1035`
- 3  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 3  `trypoptask @ ./task.jl:1208`
log category time (s): Dict("compute" => 0.000164794, "schedule" => 0.000988604, "add_thunk" => 0.001216357, "fire" => 0.00016216800000000002, "proc_run_wait" => 0.0016827429999999998, "move" => 0.00031513099999999996, "enqueue" => 2.9191e-5, "storage_safe_scan" => 1.6357e-5, "finish" => 6.644499999999999e-5, "proc_steal_local" => 0.00011325900000000001, "proc_run_fetch" => 5.359000000000001e-5, "datadeps_execute" => 0.001284018, "datadeps_copy_skip" => 0.0003307109999999999)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 9, "move" => 28, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 9, "proc_run_fetch" => 9, "datadeps_execute" => 4, "datadeps_copy_skip" => 12)

#### pc_blockjacobi_apply
samples=100
- 182  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 84  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 84  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 56  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 50  `#wait#406 @ ./condition.jl:141`
- 50  `wait @ ./condition.jl:136`
- 50  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 50  `poptask @ ./task.jl:1216`
- 50  `wait @ ./task.jl:1228`
- 48  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 47  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 47  `wait @ ./lock.jl:623`
- 47  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 36  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 16  ` @ :-1`
- 11  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 11  `trypoptask @ ./task.jl:1208`
- 11  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 11  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 6  `trylock @ ./locks-mt.jl:53`
- 6  `multiq_deletemin @ ./partr.jl:202`
- 3  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 3  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 3  `take! @ ./channels.jl:526`
log category time (s): Dict("compute" => 0.00039323799999999996, "schedule" => 0.001044556, "add_thunk" => 0.001259965, "fire" => 0.000161918, "proc_run_wait" => 0.0024240549999999996, "move" => 0.000293253, "enqueue" => 4.461e-5, "storage_safe_scan" => 1.3613e-5, "finish" => 2.4543e-5, "proc_steal_local" => 0.000108944, "proc_run_fetch" => 5.6277e-5, "datadeps_execute" => 0.001328325, "datadeps_copy_skip" => 7.5711e-5)
log category count: Dict("compute" => 4, "schedule" => 4, "add_thunk" => 4, "fire" => 4, "proc_run_wait" => 9, "move" => 24, "enqueue" => 4, "storage_safe_scan" => 4, "finish" => 4, "proc_steal_local" => 9, "proc_run_fetch" => 9, "datadeps_execute" => 4, "datadeps_copy_skip" => 8)

#### assembly
samples=677
- 1219  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 561  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 561  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 398  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 337  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 334  `#wait#406 @ ./condition.jl:141`
- 334  `wait @ ./task.jl:1228`
- 334  `wait @ ./condition.jl:136`
- 333  `poptask @ ./task.jl:1216`
- 318  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 314  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 313  `wait @ ./lock.jl:623`
- 312  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 243  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 112  ` @ :-1`
- 76  `trypoptask @ ./task.jl:1208`
- 76  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 76  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 76  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 41  `multiq_deletemin @ ./partr.jl:202`
- 35  `trylock @ ./locks-mt.jl:53`
- 23  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 23  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 22  `jl_repl_entrypoint @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/jlapi.c:1153`
log category time (s): Dict("compute" => 0.07101569599999996, "schedule" => 0.17956661799999976, "add_thunk" => 0.27682106300000003, "fire" => 0.04457989199999997, "proc_run_wait" => 2.543264012000002, "move" => 0.11801182200000009, "enqueue" => 0.014303407999999995, "storage_safe_scan" => 0.014709580000000003, "finish" => 0.012294640000000003, "proc_steal_local" => 0.4898062239999994, "proc_run_fetch" => 0.6201780389999999)
log category count: Dict("compute" => 512, "schedule" => 512, "add_thunk" => 512, "fire" => 512, "proc_run_wait" => 7950, "move" => 2048, "enqueue" => 512, "storage_safe_scan" => 512, "finish" => 512, "proc_steal_local" => 7033, "proc_run_fetch" => 8245)

#### sparse_chol_apply
samples=14
- 20  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 14  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 10  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 10  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 9  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 8  `#wait#406 @ ./condition.jl:141`
- 8  `wait @ ./condition.jl:136`
- 8  `poptask @ ./task.jl:1216`
- 8  `wait @ ./task.jl:1228`
- 7  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 7  `wait @ ./lock.jl:623`
- 7  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 5  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 5  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 3  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 3  `trypoptask @ ./task.jl:1208`
- 3  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 3  ` @ :-1`
- 3  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 3  `jl_f__apply_iterate @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:868`
- 2  `jl_f_invokelatest @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:881`
- 2  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 2  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 1  `execute! @ /home/ubuntu/work/Dagger.jl/src/threadproc.jl:13`
log category time (s): Dict("compute" => 0.000900016, "schedule" => 0.000874737, "add_thunk" => 0.001171191, "fire" => 0.00039839799999999995, "proc_run_wait" => 0.0023721709999999997, "move" => 0.00019162299999999998, "enqueue" => 9.986899999999999e-5, "storage_safe_scan" => 3.3193000000000006e-5, "finish" => 5.3012e-5, "proc_steal_local" => 0.00040917, "proc_run_fetch" => 0.00032719400000000005)
log category count: Dict("compute" => 6, "schedule" => 6, "add_thunk" => 6, "fire" => 6, "proc_run_wait" => 19, "move" => 17, "enqueue" => 6, "storage_safe_scan" => 6, "finish" => 6, "proc_steal_local" => 18, "proc_run_fetch" => 20)

#### sparse_chol_factor_solve
samples=18151
- 41918  ` @ :-1`
- 40837  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 33946  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 18696  `jl_f__apply_iterate @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:868`
- 16052  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 16052  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 12464  `jl_f_invokelatest @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:881`
- 9067  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 8464  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 6411  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 6411  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 6232  `execute! @ /home/ubuntu/work/Dagger.jl/src/threadproc.jl:13`
- 6232  `DoTaskSpec @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1957`
- 6232  `with_options @ /home/ubuntu/work/Dagger.jl/src/options.jl:258`
- 6232  `#do_task##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:2417`
- 6232  `jfptr_with_options_25194 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 6232  `#execute!#48 @ /home/ubuntu/work/Dagger.jl/src/threadproc.jl:23`
- 6232  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:639`
- 6232  `macro expansion @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:2412`
- 6232  `do_task @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:51`
- 6229  `collect_to_with_first! @ ./array.jl:826`
- 6229  `_collect @ ./array.jl:820`
- 6229  `collect_similar @ ./array.jl:732`
- 6229  `map @ ./abstractarray.jl:3375`
log category time (s): Dict("compute" => 26.054805984999994, "schedule" => 0.0075767939999999995, "add_thunk" => 0.010138972999999999, "fire" => -0.0024701110000000005, "proc_run_wait" => 29.390480252000007, "move" => 0.0013554839999999995, "enqueue" => 0.000371349, "storage_safe_scan" => 0.00067192, "finish" => 0.0003498030000000001, "proc_steal_local" => 0.007874774, "proc_run_fetch" => 0.009060093000000002)
log category count: Dict("compute" => 32, "schedule" => 32, "add_thunk" => 32, "fire" => 32, "proc_run_wait" => 159, "move" => 124, "enqueue" => 32, "storage_safe_scan" => 31, "finish" => 32, "proc_steal_local" => 136, "proc_run_fetch" => 170)

#### projected
samples=397
- 705  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 319  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 319  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 227  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 199  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 195  `wait @ ./condition.jl:136`
- 190  `#wait#406 @ ./condition.jl:141`
- 190  `wait @ ./task.jl:1228`
- 190  `poptask @ ./task.jl:1216`
- 188  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 180  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 179  `wait @ ./lock.jl:623`
- 179  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 131  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 82  ` @ :-1`
- 52  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 51  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 46  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
- 43  `trypoptask @ ./task.jl:1208`
- 19  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 19  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 16  `take! @ ./channels.jl:526`
- 16  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:630`
- 16  `take_buffered @ ./channels.jl:532`
log category time (s): Dict("compute" => 0.008459036, "schedule" => 0.016062616000000005, "add_thunk" => 0.018475178999999998, "fire" => 0.007543578999999999, "proc_run_wait" => 0.349079977, "move" => 0.036305728000000016, "enqueue" => 0.0021375519999999996, "storage_safe_scan" => 0.0020758140000000005, "finish" => 0.002396675999999999, "proc_steal_local" => 0.07774974399999995, "proc_run_fetch" => 0.09502488500000002, "datadeps_execute" => 0.017493741000000004, "datadeps_copy_skip" => 0.0013760639999999998)
log category count: Dict("compute" => 104, "schedule" => 104, "add_thunk" => 104, "fire" => 104, "proc_run_wait" => 1415, "move" => 696, "enqueue" => 104, "storage_safe_scan" => 104, "finish" => 104, "proc_steal_local" => 1260, "proc_run_fetch" => 1457, "datadeps_execute" => 80, "datadeps_copy_skip" => 112)

#### dense_lu
samples=823
- 1516  ` @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 729  `pthread_cond_wait @ /lib/x86_64-linux-gnu/libc.so.6:-1`
- 729  `uv_cond_wait @ /workspace/srcdir/libuv/src/unix/thread.c:822`
- 577  `jl_apply @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/julia.h:2394`
- 420  `start_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/task.c:1253`
- 401  `wait @ ./condition.jl:136`
- 400  `#wait#406 @ ./condition.jl:141`
- 400  `wait @ ./task.jl:1228`
- 400  `poptask @ ./task.jl:1216`
- 384  `jl_parallel_gc_threadfun @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/gc-stock.c:3645`
- 373  `wait @ ./lock.jl:623`
- 364  `jfptr_YY.start_processor_runnerNOT.YY.YY.0_23761 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 363  `#start_processor_runner!##0 @ /home/ubuntu/work/Dagger.jl/src/sch/Sch.jl:1728`
- 345  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:523`
- 181  ` @ :-1`
- 69  `jl_f__apply_iterate @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:868`
- 56  `jfptr_YY.34_22921 @ /home/ubuntu/.julia/compiled/v1.12/Dagger/0a2f8_YBYfI.so:-1`
- 56  `#34 @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:601`
- 50  `jl_f_invokelatest @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/builtins.c:881`
- 29  `get_next_task @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:309`
- 29  `ijl_task_get_next @ /cache/build/builder-amdci5-2/julialang/julia-ci/src/scheduler.c:371`
- 28  `take! @ ./channels.jl:526`
- 28  `reusable_task_loop @ /home/ubuntu/work/Dagger.jl/src/utils/reuse.jl:639`
- 28  `jfptr_trypoptask_1806.1 @ /home/ubuntu/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/lib/julia/sys.so:-1`
log category time (s): Dict("compute" => 0.36869350499999975, "schedule" => 0.3013862659999997, "add_thunk" => 0.16845883699999994, "fire" => 0.07629444999999996, "proc_run_wait" => 3.681568790999997, "move" => 0.35405657599999996, "enqueue" => 0.03564979499999996, "storage_safe_scan" => 0.03604867699999996, "finish" => 0.03656170000000004, "proc_steal_local" => 0.654585593000003, "proc_run_fetch" => 0.9114853880000017, "datadeps_execute" => 0.16520224800000002, "datadeps_copy_skip" => 0.002646202)
log category count: Dict("compute" => 752, "schedule" => 752, "add_thunk" => 752, "fire" => 752, "proc_run_wait" => 8882, "move" => 4941, "enqueue" => 752, "storage_safe_scan" => 752, "finish" => 752, "proc_steal_local" => 7106, "proc_run_fetch" => 9442, "datadeps_execute" => 672, "datadeps_copy_skip" => 449)
