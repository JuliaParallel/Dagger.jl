module ROCSparseArraysExt

# Sparse DArray support for AMDGPU / rocSPARSE. Loaded when both AMDGPU and
# SparseArrays are available (see Project.toml combo extension).

import Dagger
import Dagger: ROCArrayDeviceProc
import AMDGPU
import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import AMDGPU: ROCArray
import AMDGPU.rocSPARSE: ROCSparseMatrixCSC, ROCSparseMatrixCSR, ROCSparseVector

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

#----- Memory / aliasing -------------------------------------------------------

Dagger.value_memory_space(x::ROCSparseMatrixCSC) = Dagger.memory_space(x.nzVal)
Dagger.value_memory_space(x::ROCSparseMatrixCSR) = Dagger.memory_space(x.nzVal)
Dagger.value_memory_space(x::ROCSparseVector) = Dagger.memory_space(x.nzVal)
# Datadeps `aliased_object!` keys slots by `memory_space(x)` (see DSparseArray).
Dagger.memory_space(x::ROCSparseMatrixCSC) = Dagger.value_memory_space(x)
Dagger.memory_space(x::ROCSparseMatrixCSR) = Dagger.value_memory_space(x)
Dagger.memory_space(x::ROCSparseVector) = Dagger.value_memory_space(x)

function Dagger.aliasing(x::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR,ROCSparseVector}, _=identity)
    space = Dagger.value_memory_space(x)
    ptr = Dagger.RemotePtr{Cvoid}(UInt(pointer_from_objref(x)), space)
    return Dagger.ObjectAliasing(ptr, sizeof(typeof(x)))
end
Dagger.aliases_as_whole(::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR,ROCSparseVector}) = true
Dagger.wraps_as_sparse_tile(::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR,ROCSparseVector}) = true

#----- Allocation --------------------------------------------------------------

Dagger.allocate_sparse_zeros(::ROCArrayDeviceProc, ::Type{T}, dims::Dims{2}) where T =
    ROCSparseMatrixCSC(SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_zeros(::ROCArrayDeviceProc, ::Type{T}, dims::Dims{1}) where T =
    ROCSparseVector(SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_rand(::ROCArrayDeviceProc, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    ROCSparseMatrixCSC(SparseArrays.sprand(T, dims..., sparsity))
Dagger.allocate_sparse_rand(::ROCArrayDeviceProc, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    ROCSparseVector(SparseArrays.sprand(T, dims..., sparsity))

#----- Similar / collect -------------------------------------------------------

Dagger._sparse_similar(::ROCSparseMatrixCSC, ::Type{T}, dims::Dims{2}) where T =
    ROCSparseMatrixCSC(SparseArrays.spzeros(T, dims...))
Dagger._sparse_similar(::ROCSparseVector, ::Type{T}, dims::Dims{1}) where T =
    ROCSparseVector(SparseArrays.spzeros(T, dims...))
Dagger._sparse_collect(A::ROCSparseMatrixCSC) = SparseMatrixCSC(A)
Dagger._sparse_collect(A::ROCSparseMatrixCSR) = SparseMatrixCSC(ROCSparseMatrixCSC(A))
Dagger._sparse_collect(A::ROCSparseVector) = SparseVector(A)
Dagger._sparse_copy(A::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR,ROCSparseVector}) = copy(A)

#----- Host ↔ device helpers ---------------------------------------------------

_to_roc_sparse(x::SparseMatrixCSC) = ROCSparseMatrixCSC(x)
_to_roc_sparse(x::SparseVector) = ROCSparseVector(x)
_to_roc_sparse(x::ROCSparseMatrixCSC) = x
_to_roc_sparse(x::ROCSparseMatrixCSR) = ROCSparseMatrixCSC(x)
_to_roc_sparse(x::ROCSparseVector) = x
_to_roc_sparse(x::Dagger.DeviceSparseMatrixCSC) = ROCSparseMatrixCSC(SparseMatrixCSC(x))
_to_roc_dsparse(x::Dagger.DSparseArray) = Dagger.DSparseArray(_to_roc_sparse(x.mat))

_to_host_sparse(x::ROCSparseMatrixCSC) = SparseMatrixCSC(x)
_to_host_sparse(x::ROCSparseMatrixCSR) = SparseMatrixCSC(ROCSparseMatrixCSC(x))
_to_host_sparse(x::ROCSparseVector) = SparseVector(x)
_to_host_sparse(x::SparseMatrixCSC) = x
_to_host_sparse(x::SparseVector) = x
_to_host_dsparse(x::Dagger.DSparseArray) = Dagger.DSparseArray(_to_host_sparse(x.mat))

_to_csc(A::ROCSparseMatrixCSC) = A
_to_csc(A::ROCSparseMatrixCSR) = ROCSparseMatrixCSC(A)
_to_csc(A) = ROCSparseMatrixCSC(A)

# Materialize transpose/adjoint as CSC so SpGEMM stays on rocSPARSE / host CSC
# paths; `A * transpose(B)` would otherwise fall into scalar-indexing generic mul.
function _op_csc(X, t::Char)
    Xc = _to_csc(X)
    t == 'N' && return Xc
    Sh = SparseMatrixCSC(Xc)
    t == 'T' && return ROCSparseMatrixCSC(SparseArrays.sparse(transpose(Sh)))
    t == 'C' && return ROCSparseMatrixCSC(SparseArrays.sparse(adjoint(Sh)))
    throw(ArgumentError("Invalid trans char: $t"))
end

#----- Move (preserve sparsity; wrap in DSparseArray) --------------------------

function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::SparseMatrixCSC)
    Dagger.with_context(to_proc) do
        return Dagger.DSparseArray(ROCSparseMatrixCSC(x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::SparseVector)
    Dagger.with_context(to_proc) do
        return Dagger.DSparseArray(ROCSparseVector(x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Dagger.DSparseArray)
    Dagger.with_context(to_proc) do
        return _to_roc_dsparse(x)
    end
end
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc, x::Dagger.DSparseArray)
    Dagger.with_context(from_proc) do
        AMDGPU.synchronize()
        return _to_host_dsparse(x)
    end
end
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc,
                     x::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR,ROCSparseVector})
    Dagger.with_context(from_proc) do
        AMDGPU.synchronize()
        return Dagger.DSparseArray(_to_host_sparse(x))
    end
end
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::ROCArrayDeviceProc, x::Dagger.DSparseArray)
    # Same device: identity (like dense ROCArray). A copy here would discard
    # in-place SpGEMM writes under Datadeps/Sch argument moves.
    if from_proc == to_proc
        Dagger.with_context(AMDGPU.synchronize, from_proc)
        return x
    end
    # Distinct devices: stage through host (no peer-copy helper yet).
    Dagger.with_context(to_proc) do
        return _to_roc_dsparse(_to_host_dsparse(x))
    end
end

#----- Tile kernels ------------------------------------------------------------

function Dagger.matmatmul!(
    C::Dagger.DSparseMatrix,
    transA::Char, transB::Char,
    A::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR},
    B::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR},
    alpha, beta
)
    AB = _to_csc(_op_csc(A, transA) * _op_csc(B, transB))
    prod = _to_csc(isone(alpha) ? AB : alpha * AB)
    if iszero(beta)
        C.mat = prod
    elseif isone(beta)
        C.mat = _to_csc(prod + _to_csc(C.mat))
    else
        C.mat = _to_csc(prod + beta * _to_csc(C.mat))
    end
    return C
end

_apply_trans(X, t::Char) =
    t == 'N' ? X : t == 'T' ? transpose(X) : t == 'C' ? adjoint(X) :
    throw(ArgumentError("Invalid trans char: $t"))

function Dagger.matvecmul!(C::ROCArray, transA::Char,
                           A::Union{ROCSparseMatrixCSC,ROCSparseMatrixCSR},
                           B::ROCArray, alpha, beta)
    # rocSPARSE SpMV supports transpose/adjoint wrappers.
    LinearAlgebra.mul!(C, _apply_trans(A, transA), B, alpha, beta)
    return C
end

function Dagger.transpose_tile(B::ROCSparseMatrixCSC)
    return ROCSparseMatrixCSC(SparseArrays.sparse(SparseMatrixCSC(B)'))
end
function Dagger.transpose_tile(B::ROCSparseMatrixCSC, uplo::Char)
    Bh = SparseMatrixCSC(B)
    Bt = uplo == 'U' ? SparseArrays.triu(Bh) :
         uplo == 'L' ? SparseArrays.tril(Bh) :
         throw(ArgumentError("uplo must be 'U' or 'L', got $uplo"))
    Ct = Bt + Bt'
    for i in 1:LinearAlgebra.checksquare(Bh)
        Ct[i, i] = Bh[i, i]
    end
    return ROCSparseMatrixCSC(Ct)
end

end # module ROCSparseArraysExt
