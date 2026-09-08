| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |
|---|---|---|---|---|---|---|
| Dense LU + \ | n=2048, tile=256×256, Float64, factor + \ | 136.55 ms | 50.55 ms (LinearAlgebra.lu(::Matrix) LAPACK getrf) | 136.55 ms / 50.55 ms | 0.37× | Dagger BLAS=1; host BLAS=16 |
| Dense Cholesky + \ | n=2048, tile=256×256, SPD G*G', factor + \ | 201.0 ms | 54.86 ms (LinearAlgebra.cholesky(::Matrix) LAPACK potrf) | 201.0 ms / 54.86 ms | 0.27× | Dagger BLAS=1; host BLAS=16 |
| Dense SVD | n=256, tile=128×128, Float64, svd only | 405.89 ms | 13.73 ms (LinearAlgebra.svd(::Matrix) LAPACK) | 405.89 ms / 13.73 ms | 0.03× | modest size (tiled Jacobi vs LAPACK gesdd); Dagger BLAS=1; host BLAS=16 |
| Krylov CG (no PC) | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.372 s | 5.45 ms (Krylov.cg(::CSC)) | 3.372 s / 5.45 ms | 0.0× | no preconditioner; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=196/196; ‖Ax−b‖/‖b‖ D/B=8.82e-9/8.82e-9 |
| Krylov GMRES (no PC) | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 77.386 s | 45.38 ms (Krylov.gmres(::CSC)) | 77.386 s / 45.38 ms | 0.0× | no preconditioner; memory=50; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=192/192; ‖Ax−b‖/‖b‖ D/B=9.6e-9/9.6e-9 |
| Krylov CG + Jacobi | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.788 s | 64.96 ms (Krylov.cg + Diagonal(1./diag)) | 3.788 s / 64.96 ms | 0.02× | host M = Diagonal (ldiv); Dagger JacobiPreconditioner (mul!); square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=196/196; ‖Ax−b‖/‖b‖ D/B=8.82e-9/8.82e-9; PC setup D=732.81 ms; B=69.0 µs |
| Krylov CG + BlockJacobi | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 845.87 ms | 56.83 ms (Krylov.cg + hand-rolled block LU) | 845.87 ms / 56.83 ms | 0.07× | host is serial per-block LU (no ecosystem BlockJacobi); square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=43/43; ‖Ax−b‖/‖b‖ D/B=4.65e-8/4.65e-8; PC setup D=472.92 ms; B=47.22 ms |
| Krylov GMRES + BlockILU | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.855 s | 1.87 ms (Krylov.gmres + IncompleteLU.ilu (whole matrix)) | 3.855 s / 1.87 ms | 0.0× | host ILU is global (stronger than per-tile); Dagger is block-diagonal ILU; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=40/8; ‖Ax−b‖/‖b‖ D/B=2.73e-7/4.56e-9; PC setup D=255.0 ms; B=4.61 ms |
| Krylov GMRES + per-tile AMG | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.762 s | 3.04 ms (Krylov.gmres + AlgebraicMultigrid RS (global)) | 3.762 s / 3.04 ms | 0.0× | Dagger AMGPreconditioner is block-diagonal (lesson 19); host is true global AMG. Check ‖Ax−b‖, not only stats.solved; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=39/5; ‖Ax−b‖/‖b‖ D/B=2.87e-7/4.74e-7; PC setup D=1.148 s; B=2.95 ms |
| krylov_globalamg | (failed) | — | — (—) | — / — | — | ERROR: MethodError: \(::Dagger.DaggerSparseLU{DTask, ProcessScope, Blocks{1}}, ::DVector{Float64, Blocks{1}, typeof(cat), Vector{DTask}, Dagger.DomainBlocks{1}}) is ambiguous.

Candidates:
  \(F::Dagger.DaggerSparseLU, b::AbstractVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:319
  \(F::Union{Dagger.DaggerSparseCholesky, Dagger.DaggerSparseLU}, b::DVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:287

Possible fix, define
  \(::Dagger.DaggerSparseLU, ::DVector)
 |
| Additive Schwarz (RAS) | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.797 s | 8.97 ms (Krylov.gmres + hand-rolled RAS (serial)) | 3.797 s / 8.97 ms | 0.0× | overlap=1, PC_ASM_RESTRICT; no Julia-ecosystem distributed RAS — host is serial pre-factored RAS; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=39/39; ‖Ax−b‖/‖b‖ D/B=3.12e-7/3.12e-7; PC setup D=361.04 ms; B=8.21 ms |
| Sparse cholesky + \ | 2-D Laplacian 80×80 (n=6400, nnz=31680), tile=1280×1280 | 2.423 s | 6.22 ms (CHOLMOD cholesky(::CSC)) | 2.423 s / 6.22 ms | 0.0× | Dagger gathers then CHOLMOD on one worker; both fit in RAM |
| sparse_chol | (failed) | — | — (—) | — / — | — | ERROR: MethodError: \(::Dagger.DaggerSparseLU{DTask, ProcessScope, Blocks{1}}, ::DVector{Float64, Blocks{1}, typeof(cat), Vector{DTask}, Dagger.DomainBlocks{1}}) is ambiguous.

Candidates:
  \(F::Dagger.DaggerSparseLU, b::AbstractVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:319
  \(F::Union{Dagger.DaggerSparseCholesky, Dagger.DaggerSparseLU}, b::DVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:287

Possible fix, define
  \(::Dagger.DaggerSparseLU, ::DVector)
 |
| Incremental sparse(I,J,V, Blocks) | 2-D Laplacian COO 200×200 (n=40000, nnz=199200), tile=2500×2500 | 111.12 ms | 58.99 ms (sparse(I,J,V) then distribute) | 111.12 ms / 58.99 ms | 0.53× | baseline includes host CSC construction + distribute |
| LinearSolve KrylovJL_GMRES | 2-D Laplacian 64×64 (n=4096), tile=1024×1024 | 77.977 s | 42.62 ms (LinearSolve KrylovJL_GMRES(::CSC)) | 77.977 s / 42.62 ms | 0.0× | same LinearSolve algorithm; Dagger defaultalg would also pick KrylovJL_GMRES for this size if forced, but we pin the alg |
| linearsolve_krylov | (failed) | — | — (—) | — / — | — | ERROR: MethodError: \(::Dagger.DaggerSparseLU{DTask, ProcessScope, Blocks{1}}, ::DVector{Float64, Blocks{1}, typeof(cat), Vector{DTask}, Dagger.DomainBlocks{1}}) is ambiguous.

Candidates:
  \(F::Dagger.DaggerSparseLU, b::AbstractVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:319
  \(F::Union{Dagger.DaggerSparseCholesky, Dagger.DaggerSparseLU}, b::DVector)
    @ Dagger ~/work/Dagger.jl/src/array/sparsedirect.jl:287

Possible fix, define
  \(::Dagger.DaggerSparseLU, ::DVector)
 |
| Projected mul! | 1-D Laplacian n=2048, tile=256, constant nullspace | 28.0 ms | 19.0 µs (serial P A P (orthonormal ones)) | 28.0 ms / 19.0 µs | 0.0× | correctness-adjacent; constructor orthonormalizes |
| BlockOperator mul! | 2-field nest n=2048 (2×1024), tile=256 | 44.59 ms | 242.2 µs (serial *(::Matrix) of assembled nest) | 44.59 ms / 242.2 µs | 0.01× | correctness-adjacent; hvcat would assemble, this stays matrix-free |

