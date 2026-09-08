module MatrixMarketExt

using MatrixMarket
using SparseArrays
using LinearAlgebra
using Dagger

_unwrap_mm(A::LinearAlgebra.Symmetric) = parent(A)
_unwrap_mm(A::LinearAlgebra.Hermitian) = parent(A)
_unwrap_mm(A) = A

"""
    MatrixMarket.mmread(filename, part::Blocks) -> DMatrix

Read a Matrix Market file and `distribute` it with `part`. Coordinate
files stay sparse tiles; array files stay dense. `Symmetric`/`Hermitian`
wrappers from the file header are unwrapped so `distribute` sees the
parent matrix.
"""
function MatrixMarket.mmread(filename::AbstractString, part::Dagger.Blocks)
    return Dagger.distribute(_unwrap_mm(MatrixMarket.mmread(filename)), part)
end

"""
    MatrixMarket.mmwrite(filename, A::DMatrix)

Write a sparse-backed `DMatrix` as Matrix Market coordinate data. Tiles
are gathered to one `SparseMatrixCSC` (no densify). A dense `DMatrix`
throws — use `DelimitedFiles.writedlm` or `mmwrite(path, sparse(collect(A)))`.
"""
function MatrixMarket.mmwrite(filename::AbstractString, A::Dagger.DMatrix)
    Dagger.is_sparse_backed(A) || throw(ArgumentError(
        "MatrixMarket.mmwrite requires a sparse matrix; a dense DMatrix \
         would have to be sparsified first. Use DelimitedFiles.writedlm \
         for dense arrays, or mmwrite(path, sparse(collect(A)))."))
    return MatrixMarket.mmwrite(filename, Dagger._collect_sparse_dmatrix(A))
end

end
