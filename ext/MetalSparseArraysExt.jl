module MetalSparseArraysExt

# Sparse DArray support for Metal via DeviceSparseMatrixCSC (no vendor sparse
# library). SpGEMM/SpMV fall back to host SparseArrays.

import Dagger
import Dagger: MtlArrayDeviceProc
import Metal
import Adapt
import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import Metal: MtlArray

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

Adapt.adapt_structure(::Type{<:MtlArray}, S::SparseMatrixCSC) =
    Dagger.device_sparse_from_host(MtlArray, S)

Dagger.allocate_sparse_zeros(::MtlArrayDeviceProc, ::Type{T}, dims::Dims{2}) where T =
    Dagger.device_sparse_from_host(MtlArray, SparseArrays.spzeros(T, dims...))
Dagger.allocate_sparse_zeros(::MtlArrayDeviceProc, ::Type{T}, dims::Dims{1}) where T =
    SparseArrays.spzeros(T, dims...)
Dagger.allocate_sparse_rand(::MtlArrayDeviceProc, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    Dagger.device_sparse_from_host(MtlArray, SparseArrays.sprand(T, dims..., sparsity))
Dagger.allocate_sparse_rand(::MtlArrayDeviceProc, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    SparseArrays.sprand(T, dims..., sparsity)

function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::SparseMatrixCSC)
    Dagger.with_context(to_proc) do
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(MtlArray, x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::SparseVector)
    return Dagger.DSparseArray(copy(x))
end
function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::Dagger.DSparseArray)
    Dagger.with_context(to_proc) do
        mat = x.mat
        S = mat isa SparseMatrixCSC ? mat : SparseMatrixCSC(mat)
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(MtlArray, S))
    end
end
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::CPUProc, x::Dagger.DSparseArray)
    Dagger.with_context(from_proc) do
        Metal.synchronize()
        mat = x.mat
        if mat isa Dagger.DeviceSparseMatrixCSC
            return Dagger.DSparseArray(SparseMatrixCSC(mat))
        else
            return Dagger.DSparseArray(copy(mat))
        end
    end
end
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::MtlArrayDeviceProc, x::Dagger.DSparseArray)
    if from_proc == to_proc
        return x
    end
    Dagger.with_context(to_proc) do
        S = x.mat isa Dagger.DeviceSparseMatrixCSC ? SparseMatrixCSC(x.mat) : SparseMatrixCSC(x.mat)
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(MtlArray, S))
    end
end

end # module MetalSparseArraysExt
