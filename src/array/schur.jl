# Dense Schur for a `DMatrix`. LinearAlgebra's generic `schur(::AbstractMatrix)`
# is `schur!(Matrix(A))`. `Matrix(::DArray)` scalar-indexes (one task per
# element). `eigen(::DMatrix)` is LOBPCG for a few pairs (lesson 43) and is
# not replaced here. Full ScaLAPACK geev is out of scope.
#
# Sparse-backed tiles must not reach `collect` — that densifies the operator.

function LinearAlgebra.schur(A::DMatrix)
    LinearAlgebra.checksquare(A)
    if is_sparse_backed(A)
        throw(ArgumentError(
            "schur of a sparse-backed DMatrix would densify the operator. \
             Use eigen(A; nev=...) for a few pairs (LOBPCG), or collect \
             only if you intend a dense Schur."))
    end
    return LinearAlgebra.schur!(collect(A))
end

function LinearAlgebra.schur!(A::DMatrix)
    # Not in-place on tiles: a tiled Schur is ScaLAPACK-class work this
    # path does not do. Same gather as `schur(::DMatrix)`.
    return LinearAlgebra.schur(A)
end
