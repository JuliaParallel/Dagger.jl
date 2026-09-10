# Dagger linalg pitch videos

Six short clips for a collaborator pitch: **Dagger is a distributed linear-algebra
provider**, not a Laplacian colorizer. Each clip uses a different public API
from the 2026 linalg stack, checks the true residual `||r|| / ||b||` (not only
Krylov `stats.solved`), and is a loop-friendly 1080p H.264 mp4.

Videos live in [`videos/`](videos/). Frames are thrown away after `ffmpeg`.

## Pitch script (read while they play)

Dagger now speaks the language a PDE code actually uses. You assemble a sparse
operator with `sparse(I, J, V, Blocks)`, keep it tiled, and solve it with the
same Krylov / LinearSolve entry points you already know. Geometric multigrid
handles structured implicit heat. Smoothed aggregation takes rigid-body
near-nullspace for elasticity. Restricted additive Schwarz plus GMRES treats
nonsymmetric convection. `BlockOperator` nests two physics without concatenating
tiles, and field-split `BlockDiagonalPC` preconditions them. Unstructured
numbering is a METIS permutation of ordinary `Blocks`, then GlobalAMG. Mixed
precision is an FP32 block Jacobi in front of an FP64 Krylov loop. None of this
is a new `Dagger.solve` — it is Base, LinearAlgebra, SparseArrays, Krylov, and
AlgebraicMultigrid, dispatched on `DArray`.

## Clips

| File | Story | API | Tiles | `\|\|r\|\|/\|\|b\|\|` |
|---|---|---|---|---|
| [`videos/01_heat.mp4`](videos/01_heat.mp4) | Orbiting heat source, implicit backward-Euler | `GeometricMultigrid` + `Krylov.cg` (hierarchy reused) | `Blocks(400, 400)` on 40×40 | ~5e-11 / step |
| [`videos/02_elasticity.mp4`](videos/02_elasticity.mp4) | Q1 cantilever, warped mesh | `SmoothedAggregationPreconditioner(A; nullspace=N)` + GMRES | `Blocks(56, 56)` on 16×6 Q1 | 9.65e-9 (12 iters) |
| [`videos/03_convection.mp4`](videos/03_convection.mp4) | Gyre plume, nonsymmetric | `AdditiveSchwarzPreconditioner` RAS + `Krylov.gmres` | `Blocks(324, 324)` on 36×36 | ~3e-8 / step |
| [`videos/04_multiphysics.mp4`](videos/04_multiphysics.mp4) | Heat drives a membrane | `BlockOperator` + `BlockDiagonalPC` + GMRES | `Blocks(144, 144)` x 2 fields, 24x24 | 1.19e-8 |
| [`videos/05_unstructured.mp4`](videos/05_unstructured.mp4) | Disk Poisson, bad numbering → METIS → AMG | `partitioner=Metis` + `GlobalAMG` | `Blocks(220, 220)`, 4 parts | 1.18e-10 (16 CG iters) |
| [`videos/06_mixedprec.mp4`](videos/06_mixedprec.mp4) | FP32 PC, FP64 Krylov | `BlockJacobiPreconditioner(::DMatrix{Float32})` + `Krylov.cg` | `Blocks(576, 576)` on 48×48 | 3.53e-10 (44 iters) |

AIR interpolation is not on this branch; convection is honest RAS + GMRES.
Convection–diffusion is the nonsymmetric clip. Elasticity is the near-nullspace
clip (three rigid modes). Heat is geometric MG, not aggregation.

## Regenerate

From the Dagger checkout (this folder’s `Project.toml` `dev`s `../..`):

```bash
julia --project=contrib/linalg-pitch -t 8 contrib/linalg-pitch/render_all.jl
# or a subset:
julia --project=contrib/linalg-pitch -t 8 contrib/linalg-pitch/render_all.jl --solve-only
PITCH_FRAMES=3 julia --project=contrib/linalg-pitch -t 8 contrib/linalg-pitch/render_all.jl heat
```

Needs `ffmpeg` on `PATH`, plus the pitch project deps (CairoMakie, Krylov,
AlgebraicMultigrid, Metis). First run instantiates and precompiles.

Environment knobs: `PITCH_FPS` (default 24), `PITCH_SECS` (default 15),
`PITCH_FRAMES` (overrides `fps * secs`).

Do **not** run the Dagger test suite on the workstation to regenerate these.
The solves here are the small pitch problems only.

## Style

Dark stage (`#070A10`), one accent per physics (ember / mint / cyan / violet /
gold / ice), shared chrome: accent hairline, title, “solved with Dagger …”,
residual + `Blocks` + clock. 1920×1080, `libx264`, `yuv420p`, CRF 21.

## SHA

Recorded at render time in `results.md` (`git rev-parse`). The clips were
produced from this checkout of `Dagger-linalg-ultra` / `linalg/pitch-videos`.
