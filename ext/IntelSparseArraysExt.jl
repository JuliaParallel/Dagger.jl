module IntelSparseArraysExt

# Sparse DArray support for oneAPI via DeviceSparseMatrixCSC (no vendor sparse
# library assumed). SpGEMM/SpMV fall back to host SparseArrays.

import Dagger
import oneAPI
import Adapt
import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import oneAPI: oneArray

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

const IntelExt = Base.get_extension(Dagger, :IntelExt)::Module
using .IntelExt: oneArrayDeviceProc
_with_context(f, proc) = IntelExt.with_context(f, proc)

Adapt.adapt_structure(::Type{<:oneArray}, S::SparseMatrixCSC) =
    Dagger.device_sparse_from_host(oneArray, S)

Dagger.allocate_sparse_zeros(::oneArrayDeviceProc, ::Type{T}, dims::Dims{2}) where T =
    Dagger.device_sparse_from_host(oneArray, SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_zeros(::oneArrayDeviceProc, ::Type{T}, dims::Dims{1}) where T =
    SparseArrays.spzeros(T, dims...)
Dagger.allocate_sparse_rand(::oneArrayDeviceProc, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    Dagger.device_sparse_from_host(oneArray, SparseArrays.sprand(T, dims..., sparsity))
Dagger.allocate_sparse_rand(::oneArrayDeviceProc, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    SparseArrays.sprand(T, dims..., sparsity)

function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::SparseMatrixCSC)
    _with_context(to_proc) do
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(oneArray, x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::SparseVector)
    return Dagger.DSparseArray(copy(x))
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Dagger.DSparseArray)
    _with_context(to_proc) do
        mat = x.mat
        S = mat isa SparseMatrixCSC ? mat : SparseMatrixCSC(mat)
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(oneArray, S))
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::Dagger.DSparseArray)
    _with_context(from_proc) do
        oneAPI.synchronize()
        mat = x.mat
        if mat isa Dagger.DeviceSparseMatrixCSC
            return Dagger.DSparseArray(SparseMatrixCSC(mat))
        else
            return Dagger.DSparseArray(copy(mat))
        end
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::oneArrayDeviceProc, x::Dagger.DSparseArray)
    if from_proc == to_proc
        return x
    end
    _with_context(to_proc) do
        S = x.mat isa Dagger.DeviceSparseMatrixCSC ? SparseMatrixCSC(x.mat) : SparseMatrixCSC(x.mat)
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(oneArray, S))
    end
end

end # module IntelSparseArraysExt
