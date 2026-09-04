module MPISparseExt

# MPI x SparseArrays interop.
#
# Only the raw-bytes transport decomposition for `SparseMatrixCSC` lives here.
# It is kept out of `MPIExt` so that MPI acceleration does not require
# SparseArrays to be loaded: the hooks are Dagger generics (declared in
# `src/memory-spaces.jl`), and a type that provides no method simply falls back
# to full Julia serialization.
#
# N.B. This extension deliberately does not reach into `MPIExt` (via
# `Base.get_extension`): extension load order between two extensions of the same
# package is not specified, so the shared generics must live in core Dagger.

import Dagger
import SparseArrays: SparseMatrixCSC

# Three vectors + (m, n, lengths) header; `Ti` is preserved through `T` so the
# rebuilt matrix keeps the sender's index type.
function Dagger.inplace_mpi_parts(S::SparseMatrixCSC)
    isbitstype(eltype(S)) || return nothing
    return ((S.colptr, S.rowval, S.nzval),
            (S.m, S.n, length(S.colptr), length(S.rowval), length(S.nzval)))
end
function Dagger.inplace_mpi_alloc(::Type{T}, header::Tuple) where {T<:SparseMatrixCSC}
    Tv, Ti = eltype(T), T.parameters[2]
    _, _, ncolptr, nrowval, nnzval = header
    return (Vector{Ti}(undef, ncolptr),
            Vector{Ti}(undef, nrowval),
            Vector{Tv}(undef, nnzval))
end
function Dagger.inplace_mpi_build(::Type{T}, (colptr, rowval, nzval), header::Tuple) where {T<:SparseMatrixCSC}
    m, n, _, _, _ = header
    Tv, Ti = eltype(T), T.parameters[2]
    return SparseMatrixCSC{Tv,Ti}(m, n, colptr, rowval, nzval)
end

end # module MPISparseExt
