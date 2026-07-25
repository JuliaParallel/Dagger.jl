module CUDASparseArraysExt

# Sparse DArray support for CUDA / cuSPARSE. Loaded when both CUDA and
# SparseArrays are available (see Project.toml combo extension).

import Dagger
import CUDA
import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import CUDA: CuArray
import CUDA.CUSPARSE: CuSparseMatrixCSC, CuSparseMatrixCSR, CuSparseVector

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

const CUDAExt = Base.get_extension(Dagger, :CUDAExt)::Module
using .CUDAExt: CuArrayDeviceProc
_with_context(f, proc) = CUDAExt.with_context(f, proc)

#----- Memory / aliasing -------------------------------------------------------

Dagger.value_memory_space(x::CuSparseMatrixCSC) = Dagger.memory_space(x.nzVal)
Dagger.value_memory_space(x::CuSparseMatrixCSR) = Dagger.memory_space(x.nzVal)
Dagger.value_memory_space(x::CuSparseVector) = Dagger.memory_space(x.nzVal)
Dagger.memory_space(x::CuSparseMatrixCSC) = Dagger.value_memory_space(x)
Dagger.memory_space(x::CuSparseMatrixCSR) = Dagger.value_memory_space(x)
Dagger.memory_space(x::CuSparseVector) = Dagger.value_memory_space(x)

function Dagger.aliasing(x::Union{CuSparseMatrixCSC,CuSparseMatrixCSR,CuSparseVector}, _=identity)
    space = Dagger.value_memory_space(x)
    ptr = Dagger.RemotePtr{Cvoid}(UInt(pointer_from_objref(x)), space)
    return Dagger.ObjectAliasing(ptr, sizeof(typeof(x)))
end
Dagger.aliases_as_whole(::Union{CuSparseMatrixCSC,CuSparseMatrixCSR,CuSparseVector}) = true
Dagger.maybe_wrap_tile(x::Union{CuSparseMatrixCSC,CuSparseMatrixCSR,CuSparseVector}) =
    Dagger.DSparseArray(x)

#----- Allocation --------------------------------------------------------------

Dagger.allocate_sparse_zeros(::CuArrayDeviceProc, ::Type{T}, dims::Dims{2}) where T =
    CuSparseMatrixCSC(SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_zeros(::CuArrayDeviceProc, ::Type{T}, dims::Dims{1}) where T =
    CuSparseVector(SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_rand(::CuArrayDeviceProc, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    CuSparseMatrixCSC(SparseArrays.sprand(T, dims..., sparsity))
Dagger.allocate_sparse_rand(::CuArrayDeviceProc, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    CuSparseVector(SparseArrays.sprand(T, dims..., sparsity))

#----- Similar / collect -------------------------------------------------------

Dagger._sparse_similar(::CuSparseMatrixCSC, ::Type{T}, dims::Dims{2}) where T =
    CuSparseMatrixCSC(SparseArrays.spzeros(T, dims...))
Dagger._sparse_similar(::CuSparseVector, ::Type{T}, dims::Dims{1}) where T =
    CuSparseVector(SparseArrays.spzeros(T, dims...))
Dagger._sparse_collect(A::CuSparseMatrixCSC) = SparseMatrixCSC(A)
Dagger._sparse_collect(A::CuSparseMatrixCSR) = SparseMatrixCSC(CuSparseMatrixCSC(A))
Dagger._sparse_collect(A::CuSparseVector) = SparseVector(A)
Dagger._sparse_copy(A::Union{CuSparseMatrixCSC,CuSparseMatrixCSR,CuSparseVector}) = copy(A)

#----- Host ↔ device helpers ---------------------------------------------------

_to_cu_sparse(x::SparseMatrixCSC) = CuSparseMatrixCSC(x)
_to_cu_sparse(x::SparseVector) = CuSparseVector(x)
_to_cu_sparse(x::CuSparseMatrixCSC) = x
_to_cu_sparse(x::CuSparseMatrixCSR) = CuSparseMatrixCSC(x)
_to_cu_sparse(x::CuSparseVector) = x
_to_cu_sparse(x::Dagger.DeviceSparseMatrixCSC) = CuSparseMatrixCSC(SparseMatrixCSC(x))
_to_cu_dsparse(x::Dagger.DSparseArray) = Dagger.DSparseArray(_to_cu_sparse(x.mat))

_to_host_sparse(x::CuSparseMatrixCSC) = SparseMatrixCSC(x)
_to_host_sparse(x::CuSparseMatrixCSR) = SparseMatrixCSC(CuSparseMatrixCSC(x))
_to_host_sparse(x::CuSparseVector) = SparseVector(x)
_to_host_sparse(x::SparseMatrixCSC) = x
_to_host_sparse(x::SparseVector) = x
_to_host_dsparse(x::Dagger.DSparseArray) = Dagger.DSparseArray(_to_host_sparse(x.mat))

_to_csc(A::CuSparseMatrixCSC) = A
_to_csc(A::CuSparseMatrixCSR) = CuSparseMatrixCSC(A)
_to_csc(A) = CuSparseMatrixCSC(A)

function _op_csc(X, t::Char)
    Xc = _to_csc(X)
    t == 'N' && return Xc
    Sh = SparseMatrixCSC(Xc)
    t == 'T' && return CuSparseMatrixCSC(SparseArrays.sparse(transpose(Sh)))
    t == 'C' && return CuSparseMatrixCSC(SparseArrays.sparse(adjoint(Sh)))
    throw(ArgumentError("Invalid trans char: $t"))
end

_apply_trans(X, t::Char) =
    t == 'N' ? X : t == 'T' ? transpose(X) : t == 'C' ? adjoint(X) :
    throw(ArgumentError("Invalid trans char: $t"))

#----- Move --------------------------------------------------------------------

function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::SparseMatrixCSC)
    _with_context(to_proc) do
        return Dagger.DSparseArray(CuSparseMatrixCSC(x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::SparseVector)
    _with_context(to_proc) do
        return Dagger.DSparseArray(CuSparseVector(x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Dagger.DSparseArray)
    _with_context(to_proc) do
        return _to_cu_dsparse(x)
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::Dagger.DSparseArray)
    _with_context(from_proc) do
        CUDA.synchronize()
        return _to_host_dsparse(x)
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc,
                     x::Union{CuSparseMatrixCSC,CuSparseMatrixCSR,CuSparseVector})
    _with_context(from_proc) do
        CUDA.synchronize()
        return Dagger.DSparseArray(_to_host_sparse(x))
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::Dagger.DSparseArray)
    if from_proc == to_proc
        _with_context(CUDA.synchronize, from_proc)
        return x
    end
    _with_context(to_proc) do
        return _to_cu_dsparse(_to_host_dsparse(x))
    end
end

#----- Tile kernels ------------------------------------------------------------

function Dagger.matmatmul!(
    C::Dagger.DSparseMatrix,
    transA::Char, transB::Char,
    A::Union{CuSparseMatrixCSC,CuSparseMatrixCSR},
    B::Union{CuSparseMatrixCSC,CuSparseMatrixCSR},
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

function Dagger.matvecmul!(C::CuArray, transA::Char,
                           A::Union{CuSparseMatrixCSC,CuSparseMatrixCSR},
                           B::CuArray, alpha, beta)
    LinearAlgebra.mul!(C, _apply_trans(A, transA), B, alpha, beta)
    return C
end

function Dagger.transpose_tile(B::CuSparseMatrixCSC)
    return CuSparseMatrixCSC(SparseArrays.sparse(SparseMatrixCSC(B)'))
end
function Dagger.transpose_tile(B::CuSparseMatrixCSC, uplo::Char)
    Bh = SparseMatrixCSC(B)
    Bt = uplo == 'U' ? SparseArrays.triu(Bh) :
         uplo == 'L' ? SparseArrays.tril(Bh) :
         throw(ArgumentError("uplo must be 'U' or 'L', got $uplo"))
    Ct = Bt + Bt'
    for i in 1:LinearAlgebra.checksquare(Bh)
        Ct[i, i] = Bh[i, i]
    end
    return CuSparseMatrixCSC(Ct)
end

end # module CUDASparseArraysExt
