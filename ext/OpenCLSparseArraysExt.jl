module OpenCLSparseArraysExt

# Sparse DArray support for OpenCL via DeviceSparseMatrixCSC (no vendor sparse
# library). SpGEMM/SpMV fall back to host SparseArrays.

import Dagger
import Dagger: CLArrayDeviceProc
import OpenCL
import Adapt
import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import OpenCL: CLArray

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

# Do not densify via Adapt (generic `adapt(CLArray, SparseMatrixCSC)` would).
Adapt.adapt_structure(::Type{<:CLArray}, S::SparseMatrixCSC) =
    Dagger.device_sparse_from_host(CLArray, S)

_empty_device_sparse(::Type{T}, dims::Dims{2}) where T =
    Dagger.device_sparse_from_host(CLArray, SparseArrays.spzeros(T, dims...))
_empty_device_sparse(::Type{T}, dims::Dims{1}) where T =
    # Store 1-D sparse as a 1-column DeviceSparseMatrixCSC
    Dagger.device_sparse_from_host(CLArray, SparseMatrixCSC(SparseArrays.spzeros(T, dims...)))

Dagger.allocate_sparse_zeros(::CLArrayDeviceProc, ::Type{T}, dims::Dims{2}) where T =
    _empty_device_sparse(T, dims)
Dagger.allocate_sparse_zeros(::CLArrayDeviceProc, ::Type{T}, dims::Dims{1}) where T =
    SparseArrays.spzeros(T, dims...)  # vectors stay host CSC; wrap still applies
Dagger.allocate_sparse_rand(::CLArrayDeviceProc, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    Dagger.device_sparse_from_host(CLArray, SparseArrays.sprand(T, dims..., sparsity))
Dagger.allocate_sparse_rand(::CLArrayDeviceProc, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    SparseArrays.sprand(T, dims..., sparsity)

function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::SparseMatrixCSC)
    Dagger.with_context(to_proc) do
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(CLArray, x))
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::SparseVector)
    # Keep SparseVector on host (wrapped); OpenCL SpMV host-falls-back anyway.
    return Dagger.DSparseArray(copy(x))
end
function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::Dagger.DSparseArray)
    Dagger.with_context(to_proc) do
        mat = x.mat
        if mat isa SparseMatrixCSC
            return Dagger.DSparseArray(Dagger.device_sparse_from_host(CLArray, mat))
        elseif mat isa Dagger.DeviceSparseMatrixCSC
            return Dagger.DSparseArray(Dagger.device_sparse_from_host(CLArray, SparseMatrixCSC(mat)))
        else
            return Dagger.DSparseArray(Dagger.device_sparse_from_host(CLArray, SparseMatrixCSC(mat)))
        end
    end
end
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CPUProc, x::Dagger.DSparseArray)
    Dagger.with_context(from_proc) do
        OpenCL.cl.finish(OpenCL.cl.queue())
        mat = x.mat
        if mat isa Dagger.DeviceSparseMatrixCSC
            return Dagger.DSparseArray(SparseMatrixCSC(mat))
        else
            return Dagger.DSparseArray(copy(mat))
        end
    end
end
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CLArrayDeviceProc, x::Dagger.DSparseArray)
    if from_proc == to_proc
        return x
    end
    Dagger.with_context(to_proc) do
        mat = x.mat
        S = mat isa Dagger.DeviceSparseMatrixCSC ? SparseMatrixCSC(mat) : SparseMatrixCSC(mat)
        return Dagger.DSparseArray(Dagger.device_sparse_from_host(CLArray, S))
    end
end

end # module OpenCLSparseArraysExt
