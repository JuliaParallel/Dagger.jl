import Base: cat
import Random: MersenneTwister, rand!, randn!
export partition

mutable struct AllocateArray{T,N} <: ArrayOp{T,N}
    eltype::Type{T}
    f
    want_index::Bool
    domain::ArrayDomain{N}
    domainchunks
    partitioning::AbstractBlocks
end
size(a::AllocateArray) = size(a.domain)

function _cumlength(len, step)
    nice_pieces = div(len, step)
    extra = rem(len, step)
    ps = [step for i=1:nice_pieces]
    cumsum(extra > 0 ? vcat(ps, extra) : ps)
end

function partition(p::AbstractBlocks, dom::ArrayDomain)
    DomainBlocks(map(first, indexes(dom)),
        map(_cumlength, map(length, indexes(dom)), p.blocksize))
end

allocate_array_undef(T, sz) = allocate_array_undef(task_processor(), T, sz)
allocate_array_undef(_::Processor, T, sz) = Array{T,length(sz)}(undef, sz...)
function allocate_array!(A, f, idx)
    new_f! = allocate_array_func(task_processor(), f)
    new_f!(A, idx)
    return
end
function allocate_array!(A, f)
    new_f! = allocate_array_func(task_processor(), f)
    new_f!(A)
    return
end
allocate_array_func(::Processor, f) = f
@warn "Do parallel allocation" maxlog=1
function stage(ctx, A::AllocateArray)
    tasks = Array{DTask,ndims(A.domainchunks)}(undef, size(A.domainchunks)...)
    Dagger.spawn_datadeps() do
        for (i, x) in enumerate(A.domainchunks)
            task = Dagger.@spawn allocate_array_undef(A.eltype, size(x))
            if A.want_index
                Dagger.@spawn allocate_array!(Out(task), A.f, i)
            else
                Dagger.@spawn allocate_array!(Out(task), A.f)
            end
            tasks[i] = task
        end
    end
    return DArray(A.eltype, A.domain, A.domainchunks, tasks, A.partitioning)
end

const BlocksOrAuto = Union{Blocks{N} where N, AutoBlocks}

function Base.rand(p::Blocks, eltype::Type, dims::Dims)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, rand!, false, d, partition(p, d), p)
    return _to_darray(a)
end
Base.rand(p::BlocksOrAuto, T::Type, dims::Integer...) = rand(p, T, dims)
Base.rand(p::BlocksOrAuto, T::Type, dims::Dims) = rand(p, T, dims)
Base.rand(p::BlocksOrAuto, dims::Integer...) = rand(p, Float64, dims)
Base.rand(p::BlocksOrAuto, dims::Dims) = rand(p, Float64, dims)
Base.rand(::AutoBlocks, eltype::Type, dims::Dims) =
    rand(auto_blocks(dims), eltype, dims)

function Base.randn(p::Blocks, eltype::Type, dims::Dims)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, randn!, false, d, partition(p, d), p)
    return _to_darray(a)
end
Base.randn(p::BlocksOrAuto, T::Type, dims::Integer...) = randn(p, T, dims)
Base.randn(p::BlocksOrAuto, T::Type, dims::Dims) = randn(p, T, dims)
Base.randn(p::BlocksOrAuto, dims::Integer...) = randn(p, Float64, dims)
Base.randn(p::BlocksOrAuto, dims::Dims) = randn(p, Float64, dims)
Base.randn(::AutoBlocks, eltype::Type, dims::Dims) =
    randn(auto_blocks(dims), eltype, dims)

struct DArrayInnerSPRAND!{T<:AbstractFloat}
    sparsity::T
end
function (s::DArrayInnerSPRAND!)(A)
    error("FIXME")
end
function sprand(p::Blocks, eltype::Type, dims::Dims, sparsity::AbstractFloat)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, DArrayInnerSPRAND!(sparsity)#=(T, _dims) -> sprand(T, _dims..., sparsity)=#, false, d, partition(p, d), p)
    return _to_darray(a)
end
sprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...) =
    sprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end])
sprand(p::BlocksOrAuto, T::Type, dims::Dims, sparsity::AbstractFloat) =
    sprand(p, T, dims, sparsity)
sprand(p::BlocksOrAuto, dims_and_sparsity::Real...) =
    sprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end])
sprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat) =
    sprand(p, Float64, dims, sparsity)
sprand(::AutoBlocks, eltype::Type, dims::Dims, sparsity::AbstractFloat) =
    sprand(auto_blocks(dims), eltype, dims, sparsity)

function darray_inner_ones!(A)
    for idx in eachindex(A)
        A[idx] = one(eltype(A))
    end
end
function Base.ones(p::Blocks, eltype::Type, dims::Dims)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, darray_inner_ones!, false, d, partition(p, d), p)
    return _to_darray(a)
end
Base.ones(p::BlocksOrAuto, T::Type, dims::Integer...) = ones(p, T, dims)
Base.ones(p::BlocksOrAuto, T::Type, dims::Dims) = ones(p, T, dims)
Base.ones(p::BlocksOrAuto, dims::Integer...) = ones(p, Float64, dims)
Base.ones(p::BlocksOrAuto, dims::Dims) = ones(p, Float64, dims)
Base.ones(::AutoBlocks, eltype::Type, dims::Dims) =
    ones(auto_blocks(dims), eltype, dims)

function darray_inner_zeros!(A)
    for idx in eachindex(A)
        A[idx] = zero(eltype(A))
    end
end
function Base.zeros(p::Blocks, eltype::Type, dims::Dims)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, darray_inner_zeros!, false, d, partition(p, d), p)
    return _to_darray(a)
end
Base.zeros(p::BlocksOrAuto, T::Type, dims::Integer...) = zeros(p, T, dims)
Base.zeros(p::BlocksOrAuto, T::Type, dims::Dims) = zeros(p, T, dims)
Base.zeros(p::BlocksOrAuto, dims::Integer...) = zeros(p, Float64, dims)
Base.zeros(p::BlocksOrAuto, dims::Dims) = zeros(p, Float64, dims)
Base.zeros(::AutoBlocks, eltype::Type, dims::Dims) =
    zeros(auto_blocks(dims), eltype, dims)

function Base.zero(x::DArray{T,N}) where {T,N}
    dims = ntuple(i->x.domain.indexes[i].stop, N)
    sd = first(x.subdomains)
    part_size = ntuple(i->sd.indexes[i].stop, N)
    a = zeros(Blocks(part_size...), T, dims)
    return _to_darray(a)
end

function Base.view(A::AbstractArray{T,N}, p::Blocks{N}) where {T,N}
    d = ArrayDomain(Base.index_shape(A))
    dc = partition(p, d)
    # N.B. We use `tochunk` because we only want to take the view locally, and
    # taking views should be very fast
    chunks = [tochunk(view(A, x.indexes...)) for x in dc]
    return DArray(T, d, dc, chunks, p)
end
