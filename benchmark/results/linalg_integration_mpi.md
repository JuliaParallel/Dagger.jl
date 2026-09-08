| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |
|---|---|---|---|---|---|---|
| Dense GEMM / mul! | n=2048, tile=1024×1024, Float64, C←A*B | 261.17 ms | 120.27 ms (LinearAlgebra.*(::Matrix, ::Matrix) OpenBLAS) | 261.17 ms / 120.27 ms | 0.46× | Dagger BLAS=1; host BLAS=4; A*B not mul!(C,A,A) (MPI aliasing) |
| Dense LU + \ | n=1024, tile=512×512, Float64, factor + \ | 2.002 s | 16.44 ms (LinearAlgebra.lu(::Matrix) LAPACK getrf) | 2.002 s / 16.44 ms | 0.01× | Dagger BLAS=1; host BLAS=4 |
| dense_chol | (failed) | — | — (—) | — / — | — | ERROR: [rank 0][tag 50912] Hit hang on recv (dest: 3) |
| sparse_spmv | (failed) | — | — (—) | — / — | — | ERROR: [rank 0][tag 51120] Hit hang on recv (dest: 3) |
| Sparse SpGEMM | sprand n=1600, p=0.008, nnz=20561, tile=800×800 | 14.64 ms | 4.22 ms (SparseArrays *(::CSC, ::CSC)) | 14.64 ms / 4.22 ms | 0.29× | host CSC×CSC is single-threaded |
| sparse_direct | (failed) | — | — (—) | — / — | — | ERROR: [rank 0][tag 51544] Hit hang on send (dest: 3) |
| assembly | (failed) | — | — (—) | — / — | — | ERROR: DTaskFailedException:
  Root Exception Type: ErrorException
  Root Exception:
[rank 0][tag 51835] Hit hang on bcast_meta delivery (dest: 3)
Stacktrace:
  [1] error(s::String)
    @ Base ./error.jl:44
  [2] mpi_deadlock_detect(detect::Bool, time_start::UInt64, warn_period::UInt64, timeout_period::UInt64, rank::Int64, tag::UInt32, kind::String, srcdest::Int64)
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:1127
  [3] macro expansion
    @ ~/work/Dagger.jl/ext/MPIExt.jl:1272 [inlined]
  [4] macro expansion
    @ ./lock.jl:376 [inlined]
  [5] bcast_slot_wait(state::MPIExt.BcastState, root::Int64, tag::UInt32)
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:1264
  [6] bcast_meta_yield(comm::MPI.Comm, root::Int64, tag::UInt32, value::Nothing)
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:1431
  [7] bcast_meta_yield(comm::MPI.Comm, root::Int64, tag::UInt32)
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:1417
  [8] execute!(::MPIExt.MPIProcessor{Dagger.ThreadProc}, ::Function, ::Function, ::Vararg{Any}; kwargs::@Kwargs{})
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:2291
  [9] execute!(::MPIExt.MPIProcessor{Dagger.ThreadProc}, ::Function, ::Function, ::Vararg{Any})
    @ MPIExt ~/work/Dagger.jl/ext/MPIExt.jl:2206
 [10] (::Dagger.Sch.var"#do_task##0#do_task##1"{MPIExt.MPIProcessor{Dagger.ThreadProc}, Vector{Pair{Symbol, Any}}, Vector{Any}})()
    @ Dagger.Sch ~/work/Dagger.jl/src/sch/Sch.jl:2417
 [11] with_options(f::Dagger.Sch.var"#do_task##0#do_task##1"{MPIExt.MPIProcessor{Dagger.ThreadProc}, Vector{Pair{Symbol, Any}}, Vector{Any}}, options::@NamedTuple{})
    @ Dagger ~/work/Dagger.jl/src/options.jl:258
 [12] macro expansion
    @ ~/work/Dagger.jl/src/sch/Sch.jl:2412 [inlined]
 [13] do_task(to_proc::MPIExt.MPIProcessor{Dagger.ThreadProc}, task::Dagger.Sch.TaskSpec)
    @ Dagger.Sch ~/work/Dagger.jl/src/utils/reuse.jl:51
 [14] (::Dagger.Sch.DoTaskSpec)()
    @ Dagger.Sch ~/work/Dagger.jl/src/sch/Sch.jl:1957
 [15] reusable_task_loop
    @ ~/work/Dagger.jl/src/utils/reuse.jl:639
 [16] #34
    @ ~/work/Dagger.jl/src/utils/reuse.jl:601
  This Task:  DTask(id=51834, allocate_array(#6, Float64, (1024, 1024))) |
| Projected mul! | 1-D Laplacian n=1024, tile=512, constant nullspace | 15.89 ms | 15.3 µs (serial P A P (orthonormal ones)) | 15.89 ms / 15.3 µs | 0.0× | correctness-adjacent; constructor orthonormalizes |
| BlockOperator mul! | 2-field nest n=1024 (2×512), tile=512 | 18.59 ms | 159.2 µs (serial *(::Matrix) of assembled nest) | 18.59 ms / 159.2 µs | 0.01× | correctness-adjacent; hvcat would assemble, this stays matrix-free |
| numeric_refactor | (failed) | — | — (—) | — / — | — | ERROR: [rank 0][tag 53338] Hit hang on send (dest: 3) |
| mixed_mul | (failed) | — | — (—) | — / — | — | ERROR: [rank 0][tag 53388] Hit hang on send (dest: 3) |

