| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |
|---|---|---|---|---|---|---|
| Dense GEMM / mul! | n=4096, tile=512×512, Float64, C←A*A | 291.76 ms | 232.56 ms (LinearAlgebra.mul!(::Matrix) OpenBLAS) | 291.76 ms / 232.56 ms | 0.8× | Dagger BLAS=1; host BLAS=16 |
| dense_lu | (failed) | — | — (—) | — / — | — | ERROR: FieldError: type LU has no field `U`, available fields: `factors`, `ipiv`, `info`
Available properties: `L`, `U`, `p`, `P` |
| Dense QR + \ | n=2048, tile=256×256, Float64, factor + \ | 546.26 ms | 110.83 ms (LinearAlgebra.qr(::Matrix) LAPACK geqrf) | 546.26 ms / 110.83 ms | 0.2× | Dagger BLAS=1; host BLAS=16 |
| dense_chol | (failed) | — | — (—) | — / — | — | ERROR: FieldError: type Cholesky has no field `U`, available fields: `factors`, `uplo`, `info`
Available properties: `U`, `L`, `UL` |
| dense_svd | (failed) | — | — (—) | — / — | — | ERROR: FieldError: type SVD has no field `V`, available fields: `U`, `S`, `Vt`
Available properties: `V` |
| Sparse SpMV | 1-D Laplacian n=160000, nnz=479998, tile=20000 | 8.92 ms | 592.2 µs (SparseArrays *(::CSC, ::Vector)) | 8.92 ms / 592.2 µs | 0.07× | host CSC SpMV is single-threaded |
| Sparse SpGEMM | sprand n=2500, p=0.008, nnz=49889, tile=625×625 | 15.96 ms | 14.34 ms (SparseArrays *(::CSC, ::CSC)) | 15.96 ms / 14.34 ms | 0.9× | host CSC×CSC is single-threaded |
| Krylov CG (no PC) | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=256×256 | 15.837 s | 4.52 ms (Krylov.cg(::CSC)) | 15.837 s / 4.52 ms | 0.0× | no preconditioner; square tiles; same atol=1.0e-10 rtol=1.0e-8 itmax=500; iters D/B=196/196; ‖Ax−b‖/‖b‖ D/B=8.82e-9/8.82e-9 |

