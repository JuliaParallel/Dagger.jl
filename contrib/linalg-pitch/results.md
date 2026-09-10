# Pitch solve results

Recorded on the render machine after the 15 s / 1080p encode. Residuals are
un-preconditioned `||A*x - b|| / ||b||` (or the implicit-step residual for
time-dependent clips). Do not treat Krylov `stats.solved` as `Ax ≈ b`.

| Clip | API | n | Blocks | \|\|r\|\|/\|\|b\|\| | iters | solve s |
|---|---|---|---|---|---|---|
| 01_heat | GeometricMultigrid + Krylov.cg, hierarchy reused each step | 1600 | Blocks(400, 400) | 5.37e-11 | 7 / step | 18.4 |
| 02_elasticity | SmoothedAggregationPreconditioner(A; nullspace=N) + Krylov.gmres | 224 | Blocks(56, 56) | 9.65e-9 | 12 | 2.81 |
| 03_convection | AdditiveSchwarzPreconditioner(overlap=1, type=:restrict) + Krylov.gmres | 1296 | Blocks(324, 324) | 3.56e-8 | 13 / step | 4.4 |
| 04_multiphysics | BlockOperator(LT, nothing, -αI, Ku) + BlockDiagonalPC + Krylov.gmres | 576 | Blocks(144, 144) | 1.19e-8 | 103 / step | 57.1 |
| 05_unstructured | distribute(; partitioner=Metis) + SmoothedAggregationPreconditioner | 880 | Blocks(220, 220) | 1.18e-10 | 16 | 0.64 |
| 06_mixedprec | BlockJacobiPreconditioner(::DMatrix{Float32}) + Krylov.cg(::DMatrix{Float64}) | 2304 | Blocks(576, 576) | 3.53e-10 | 44 | 1.06 |

Clips are 1920×1080, 24 fps, 15 s, `libx264` `yuv420p` CRF 21. Combined size ~4.1 MiB.
