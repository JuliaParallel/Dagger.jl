import AbstractFFTs

struct FFT! end
struct RFFT! end
struct IRFFT! end
struct IFFT! end

abstract type Decomposition end
struct Pencil <: Decomposition end
struct Slab <: Decomposition end

function plan_transform(transform, A, dims; kwargs...)
    if transform isa FFT!
        return AbstractFFTs.plan_fft!(A, dims; kwargs...)
    elseif transform isa IFFT!
        return AbstractFFTs.plan_ifft!(A, dims; kwargs...)
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

# 3D out of place
function fft(input::AbstractArray{T,3}, dims=(1, 2, 3); decomp::Decomposition=Pencil()) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    if decomp isa Pencil
        A = DArray(input, Blocks(N, div(N, np), div(N, np)))
        B = zeros(Blocks(div(N, np), N, div(N, np)), size(input))
        C = zeros(Blocks(div(N, np), div(N, np), N), size(input))
        _fft!(decomp, input, A, B, C; dims)
        return C
    elseif decomp isa Slab
        A = DArray(input, Blocks(N, N, div(N, np)))
        B = zeros(Blocks(div(N, np), N, N), size(input))
        _fft!(decomp, input, A, B; dims)
        return B
    else
        throw(ArgumentError("Unknown decomposition type: $decomp"))
    end
end

function _fft!(::Pencil, input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
               dims) where T
    copyto!(A, input)
    _fft!(Pencil(), A, B, C; dims)
    return
end

# 3D in place
function fft!(output::AbstractArray{T,3}, input::AbstractArray{T,3}, dims=(1, 2, 3); decomp::Decomposition=Pencil()) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    if decomp isa Pencil
        A = DArray(input, Blocks(N, div(N, np), div(N, np)))
        B = zeros(Blocks(div(N, np), N, div(N, np)), size(input))
        C = zeros(Blocks(div(N, np), div(N, np), N), size(input))
        _fft!(decomp, output, input, A, B, C; dims)
        return output
    elseif decomp isa Slab
        A = DArray(input, Blocks(N, N, div(N, np)))
        B = zeros(Blocks(div(N, np), N, N), size(input))
        _fft!(decomp, output, input, A, B; dims)
        return output
    else
        throw(ArgumentError("Unknown decomposition type: $decomp"))
    end
end
fft!(input::AbstractArray{T,3}, dims=(1, 2, 3); decomp::Decomposition=Pencil()) where T =
    fft!(input, input, dims; decomp=decomp)

function _fft!(::Pencil, output::AbstractArray{T,3}, input::AbstractArray{T,3},
               A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
               dims) where T
    copyto!(A, input)
    _fft!(Pencil(), output, input, A, B, C; dims)
    copyto!(output, C)

    return
end

function _fft!(::Pencil, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
               dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)[$idx]" apply_fft!(InOut(A_parts[idx]), FFT!(), dims[1])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[2])
        end
    end

    copyto!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_fft!(dim 3)[$idx]" apply_fft!(InOut(C_parts[idx]), FFT!(), dims[3])
        end
    end

    return
end


# 3D Slab out of place
function _fft!(::Slab, input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}; 
               dims) where T
    copyto!(A, input)
    _fft!(Slab(), A, B; dims)
    return
end

# 3D Slab in place
function _fft!(::Slab, output::AbstractArray{T,3}, input::AbstractArray{T,3},
               A::DArray{T,3}, B::DArray{T,3}; 
               dims) where T
    copyto!(A, input)
    _fft!(Slab(), A, B; dims)
    copyto!(output, B)
    return
end

function _fft!(::Slab, A::DArray{T,3}, B::DArray{T,3}; 
               dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1&2)[$idx]" apply_fft!(InOut(A_parts[idx]), FFT!(), (dims[1], dims[2]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[3])
        end
    end

    return
end

# 2D out of place
function fft(input::AbstractArray{T,2}, dims=(1, 2)) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    A = DArray(input, Blocks(N, div(N, np)))
    B = zeros(Blocks(div(N, np), N), eltype(input), size(input))
    _fft!(input, A, B; dims)
    return B
end

function _fft!(input::AbstractArray{T,2}, A::DMatrix{T}, B::DMatrix{T}; dims) where T
    copyto!(A, input)
    _fft!(A, B; dims)
    return
end

# 2D inplace
function fft!(output::AbstractArray{T,2}, input::AbstractArray{T,2}, dims=(1, 2)) where T
    N = size(input, 1)
    np = length(Dagger.compatible_processors())
    A = DArray(input, Blocks(N, div(N, np)))
    B = zeros(Blocks(div(N, np), N), eltype(input), size(input))
    _fft!(output, input, A, B; dims)
    return output
end
fft!(input::AbstractArray{T,2}, dims=(1, 2)) where T =
    fft!(input, input, dims)

function _fft!(output::AbstractArray{T,2}, input::AbstractArray{T,2}, A::DMatrix{T}, B::DMatrix{T}; 
               dims) where T
    copyto!(A, input)
    _fft!(A, B; dims)
    copyto!(output, B)

    return
end

function _fft!(A::DMatrix{T}, B::DMatrix{T}; dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)[$idx]" apply_fft!(InOut(A_parts[idx]), FFT!(), dims[1])
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), FFT!(), dims[2])
        end
    end

    return
end





# 3D Pencil out of place
function AbstractFFTs.ifft(input::AbstractArray{T,3}, dims; decomp::Decomposition=Pencil()) where T
    N = size(input, 1) 
    if decomp isa Pencil
        A = DArray(input, Blocks(N, div(N, 2), div(N, 2)))
        B = DArray(input, Blocks(div(N, 2), N, div(N, 2)))
        C = DArray(input, Blocks(div(N, 2), div(N, 2), N))

        return _ifft(input, A, B, C; dims=dims, decomp=decomp)
    else # decomp isa Slab
        N = size(input, 1) 
        A = DArray(input, Blocks(N, N, div(N, 4)))
        B = DArray(input, Blocks(div(N, 4), N, N))
        return _ifft(input, A, B; dims=dims, decomp=decomp)
    end
end

function _ifft(input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
             dims, decomp::Decomposition=Pencil()) where T
    copyto!(A, input)
    
    return _ifft(A, B, C; dims=dims, decomp=decomp)
end

function _ifft(A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
             dims, decomp::Decomposition=Pencil()) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks
    
    transforms = [IFFT!(), IFFT!(), IFFT!()]
    
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    copyto!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)[$idx]" apply_fft!(InOut(C_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    return C
end

# 3D Pencil in place
function AbstractFFTs.ifft!(output::AbstractArray{T,3}, input::AbstractArray{T,3}, dims; decomp::Decomposition=Pencil()) where T
    N = size(input, 1)
    if decomp isa Pencil
        A = DArray(input, Blocks(N, div(N, 2), div(N, 2)))
        B = DArray(input, Blocks(div(N, 2), N, div(N, 2)))
        C = DArray(input, Blocks(div(N, 2), div(N, 2), N))

        return _ifft!(output, input, A, B, C; dims=dims, decomp=decomp)
    else
        A = DArray(input, Blocks(N, N, div(N, 4)))
        B = DArray(input, Blocks(div(N, 4), N, N))

        return _ifft!(output, input, A, B; dims=dims, decomp=decomp)
    end
end

function _ifft!(output::AbstractArray{T,3}, input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
              dims, decomp::Decomposition=Pencil()) where T

    copyto!(A, input)
    _ifft!(A, B, C; dims=dims, decomp=decomp)
    copyto!(output, C)
    
    return output
end

function _ifft!(A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}; 
    dims, decomp::Decomposition=Pencil()) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks

    transforms = [IFFT!(), IFFT!(), IFFT!()]

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    copyto!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)[$idx]" apply_fft!(InOut(C_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end
    return C
end

#3D Slab out of place
function _ifft(input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}; 
             dims, decomp::Decomposition=Slab()) where T

    copyto!(A, input)
    return _ifft(A, B; dims=dims, decomp=decomp)
end

function _ifft(A::DArray{T,3}, B::DArray{T,3}; 
            dims, decomp::Decomposition=Slab()) where T
    A_parts = A.chunks
    B_parts = B.chunks
    
    transforms = [IFFT!(), IFFT!(), IFFT!()]
    
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 1&2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[1]), In([dims[1], dims[2]]))
        end
    end

    return B
end

# 3D Slab in place
function _ifft!(output::AbstractArray{T,3}, input::AbstractArray{T,3}, A::DArray{T,3}, B::DArray{T,3}; 
              dims, decomp::Decomposition=Slab()) where T

    copyto!(A, input)
    _ifft!(A, B; dims=dims, decomp=decomp)
    copyto!(output, B)
    
    return output
end

function _ifft!(A::DArray{T,3}, B::DArray{T,3}; 
    dims, decomp::Decomposition=Slab()) where T
    A_parts = A.chunks
    B_parts = B.chunks

    transforms = [IFFT!(), IFFT!(), IFFT!()]
    
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 1&2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[1]), In([dims[1], dims[2]]))
        end
    end

    return B
end

# 2D out of place 
function AbstractFFTs.ifft(input::AbstractArray{T,2}; dims) where T
    N = size(input, 1) 
    A = DArray(input, Blocks(N, div(N, 4)))
    B = DArray(input, Blocks(div(N, 4), N))
    return _ifft(input, A, B; dims=dims)
end

function _ifft(input::AbstractArray{T,2}, A::DMatrix{T}, B::DMatrix{T}; dims) where T
    copyto!(A, input)
    return _ifft(A, B; dims=dims)
end

function _ifft(A::DMatrix{T}, B::DMatrix{T}; dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    
    transforms = [IFFT!(), IFFT!()]
    
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    return B
end

# 2D in place 
function AbstractFFTs.ifft!(output::AbstractArray{T,2}, input::AbstractArray{T,2}; dims) where T
    N = size(input, 1)
    A = DArray(input, Blocks(N, div(N, 4)))
    B = DArray(input, Blocks(div(N, 4), N))

    return _ifft!(output, input, A, B; dims=dims)
end

function _ifft!(output::AbstractArray{T,2}, input::AbstractArray{T,2}, A::DMatrix{T}, B::DMatrix{T}; 
              dims) where T

    copyto!(A, input)
    _ifft!(A, B; dims=dims)
    copyto!(output, B)
    
    return output
end

function _ifft!(A::DMatrix{T}, B::DMatrix{T}; dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    transforms = [IFFT!(), IFFT!()]

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    return B
end