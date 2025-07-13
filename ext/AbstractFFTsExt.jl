module AbstractFFTsExt

import Dagger
import Dagger: DArray, DVector, DMatrix, Blocks, AutoBlocks, InOut
import AbstractFFTs
import LinearAlgebra

abstract type Decomposition end
struct Pencil <: Decomposition end
struct Slab <: Decomposition end

# High-level interface

## TODO: Add optimized 1D algorithm

## 1D out-of-place
AbstractFFTs.fft(A::DVector) = DVector(AbstractFFTs.fft(collect(A)))
AbstractFFTs.ifft(A::DVector) = DVector(AbstractFFTs.ifft(collect(A)))

## 1D in-place
function AbstractFFTs.fft!(DA::DVector{T}) where T
    A = Vector{T}(undef, length(DA))
    copyto!(A, DA)
    AbstractFFTs.fft!(A)
    copyto!(DA, A)
    return DA
end
function AbstractFFTs.ifft!(DA::DVector{T}) where T
    A = Vector{T}(undef, length(DA))
    copyto!(A, DA)
    AbstractFFTs.ifft!(A)
    copyto!(DA, A)
    return DA
end

## 2D out-of-place
function AbstractFFTs.fft(DA::DMatrix, dims=(1, 2))
    DB = similar(DA)
    _fft!(DB, DA, dims)
    return DB
end
function AbstractFFTs.ifft(DA::DMatrix, dims=(1, 2))
    DB = similar(DA)
    _ifft!(DB, DA, dims)
    return DB
end

## 2D in-place
function AbstractFFTs.fft!(DA::DMatrix{T}, dims=(1, 2)) where T
    _fft!(DA, DA, dims)
    return DA
end
function AbstractFFTs.ifft!(DA::DMatrix{T}, dims=(1, 2)) where T
    _ifft!(DA, DA, dims)
    return DA
end

## 3D out-of-place
function AbstractFFTs.fft(DA::DArray{T,3}, dims=(1, 2, 3); decomp::Union{Decomposition,Symbol}=Pencil()) where T
    DB = similar(DA)
    _decomp = _to_decomp(decomp)
    _fft!(DB, DA, dims; decomp=_decomp)
    return DB
end
function AbstractFFTs.ifft(DA::DArray{T,3}, dims=(1, 2, 3); decomp::Union{Decomposition,Symbol}=Pencil()) where T
    DB = similar(DA)
    _decomp = _to_decomp(decomp)
    _ifft!(DB, DA, dims; decomp=_decomp)
    return DB
end

## 3D in-place
function AbstractFFTs.fft!(DA::DArray{T,3}, dims=(1, 2, 3); decomp::Union{Decomposition,Symbol}=Pencil()) where T
    _decomp = _to_decomp(decomp)
    _fft!(DA, DA, dims; decomp=_decomp)
    return DA
end
function AbstractFFTs.ifft!(DA::DArray{T,3}, dims=(1, 2, 3); decomp::Union{Decomposition,Symbol}=Pencil()) where T
    _decomp = _to_decomp(decomp)
    _ifft!(DA, DA, dims; decomp=_decomp)
    return DA
end

# Mid-level interface

_to_decomp(decomp::Decomposition) = decomp
function _to_decomp(decomp::Symbol)
    if decomp == :pencil
        return Pencil()
    elseif decomp == :slab
        return Slab()
    else
        throw(ArgumentError("Unknown decomposition type: $decomp\nSupported types: :pencil, :slab"))
    end
end

## 2D
function _fft!(output::DMatrix{T}, input::DMatrix{T}, dims=(1, 2)) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    A = zeros(Blocks(N, div(N, np)), T, size(input))
    copyto!(A, input)
    B = zeros(Blocks(div(N, np), N), T, size(input))
    __fft!(A, B, dims)
    copyto!(output, B)
    return output
end
function _ifft!(output::DMatrix{T}, input::DMatrix{T}, dims=(1, 2)) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    A = zeros(Blocks(N, div(N, np)), T, size(input))
    copyto!(A, input)
    B = zeros(Blocks(div(N, np), N), T, size(input))
    __ifft!(A, B, dims)
    copyto!(output, B)
    return output
end

## 3D
function _fft!(output::DArray{T,3}, input::DArray{T,3}, dims=(1, 2, 3); decomp::Decomposition=Pencil()) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    if decomp isa Pencil
        A = zeros(Blocks(N, div(N, np), div(N, np)), T, size(input))
        B = zeros(Blocks(div(N, np), N, div(N, np)), T, size(input))
        C = zeros(Blocks(div(N, np), div(N, np), N), T, size(input))
        copyto!(A, input)
        __fft!(decomp, A, B, C, dims)
        copyto!(output, C)
        return output
    elseif decomp isa Slab
        A = zeros(Blocks(N, N, div(N, np)), T, size(input))
        B = zeros(Blocks(div(N, np), div(N, np), N), T, size(input))
        copyto!(A, input)
        __fft!(decomp, A, B, dims)
        copyto!(output, B)
        return output
    else
        throw(ArgumentError("Unknown decomposition type: $decomp"))
    end
end
function _ifft!(output::DArray{T,3}, input::DArray{T,3}, dims=(1, 2, 3); decomp::Decomposition=Pencil()) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    if decomp isa Pencil
        A = zeros(Blocks(div(N, np), div(N, np), N), T, size(input))
        B = zeros(Blocks(div(N, np), N, div(N, np)), T, size(input))
        C = zeros(Blocks(N, div(N, np), div(N, np)), T, size(input))
        copyto!(A, input)
        __ifft!(decomp, A, B, C, dims)
        copyto!(output, C)
        return output
    elseif decomp isa Slab
        A = zeros(Blocks(div(N, np), div(N, np), N), T, size(input))
        B = zeros(Blocks(N, N, div(N, np)), T, size(input))
        copyto!(A, input)
        __ifft!(decomp, A, B, dims)
        copyto!(output, B)
        return output
    end
end

# Internal functions

struct FFT! end
struct RFFT! end
struct IRFFT! end
struct IFFT! end

function plan_transform(transform, A, dims; kwargs...)
    if transform isa FFT!
        AbstractFFTs.plan_fft!(A, dims; kwargs...)
    elseif transform isa IFFT!
        AbstractFFTs.plan_ifft!(A, dims; kwargs...)
    else
        throw(ArgumentError("Unknown transform type: $transform"))
    end
end
function apply_fft!(out_part, in_part, transform, dim)
    plan = plan_transform(transform, in_part, dim)
    LinearAlgebra.mul!(out_part, plan, in_part)
    return
end
apply_fft!(inout_part, transform, dim) = apply_fft!(inout_part, inout_part, transform, dim)

## 2D
function __fft!(A::DMatrix{T}, B::DMatrix{T}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)" apply_fft!(InOut(A_parts[idx]), FFT!(), dims[1])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[2])
        end
    end

    return
end
function __ifft!(A::DMatrix{T}, B::DMatrix{T}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)" apply_fft!(InOut(A_parts[idx]), IFFT!(), dims[1])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)" apply_fft!(InOut(B_parts[idx]), IFFT!(), dims[2])
        end
    end

    return
end

## 3D
function __fft!(::Pencil, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)" apply_fft!(InOut(A_parts[idx]), FFT!(), dims[1])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[2])
        end
    end

    copyto!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_fft!(dim 3)" apply_fft!(InOut(C_parts[idx]), FFT!(), dims[3])
        end
    end

    return
end
function __fft!(::Slab, A::DArray{T,3}, B::DArray{T,3}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1&2)" apply_fft!(InOut(A_parts[idx]), FFT!(), (dims[1], dims[2]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 3)" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[3])
        end
    end

    return
end
function __ifft!(::Pencil, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)" apply_fft!(InOut(A_parts[idx]), IFFT!(), dims[3])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)" apply_fft!(InOut(B_parts[idx]), IFFT!(), dims[2])
        end
    end

    copyto!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)" apply_fft!(InOut(C_parts[idx]), IFFT!(), dims[1])
        end
    end

    return
end
function __ifft!(::Slab, A::DArray{T,3}, B::DArray{T,3}, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)" apply_fft!(InOut(A_parts[idx]), IFFT!(), dims[3])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 1&2)" apply_fft!(InOut(B_parts[idx]), IFFT!(), (dims[1], dims[2]))
        end
    end

    return
end

end # module AbstractFFTsExt
