import Base: cat
import Random: MersenneTwister, rand!, randn!
export partition

mutable struct AllocateArray{T,N} <: ArrayOp{T,N}
    eltype::Type{T}
    f
    want_index::Bool
    domain::ArrayDomain{N}
    domainchunks
    partitioning::AbstractBlocks{N}
    procgrid::Union{AbstractProcGrid{N}, Nothing}

    function AllocateArray(eltype::Type{T}, f, want_index::Bool, d::ArrayDomain{N}, domainchunks, p::AbstractBlocks{N}, assignment::Union{AssignmentType{N},Nothing} = nothing) where {T,N}
        sizeA = map(length, d.indexes)
        procgrid = build_procgrid(something(assignment, :arbitrary), Tuple(sizeA), p.blocksize, current_acceleration())
        return new{T,N}(eltype, f, want_index, d, domainchunks, p, procgrid)
    end

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

function allocate_array(f, T, idx, sz)
    new_f = allocate_array_func(task_processor(), f)
    return new_f(idx, T, sz)
end
function allocate_array(f, T, sz)
    new_f = allocate_array_func(task_processor(), f)
    return new_f(T, sz)
end
allocate_array_func(::Processor, f) = f
function stage(ctx, A::AllocateArray)
    tasks = emit_chunk_tasks!(A.domainchunks, A.procgrid, A.eltype,
        (scope, I, i) -> begin
        x = A.domainchunks[I]
        N = ndims(A.domainchunks)
        ret_type = Array{A.eltype, N}
        if A.want_index
            Dagger.@spawn compute_scope=scope return_type=ret_type allocate_array(A.f, A.eltype, i, size(x))
        else
            Dagger.@spawn compute_scope=scope return_type=ret_type allocate_array(A.f, A.eltype, size(x))
        end
    end)
    return DArray(A.eltype, A.domain, A.domainchunks, tasks, A.partitioning)
end

const BlocksOrAuto = Union{Blocks{N} where N, AutoBlocks}

function Base.rand(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, rand, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.rand(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    rand(p, T, dims; assignment)
Base.rand(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    rand(p, Float64, dims; assignment)
Base.rand(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    rand(p, Float64, dims; assignment)
Base.rand(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    rand(auto_blocks(dims), T, dims; assignment)

function Base.randn(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, randn, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.randn(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    randn(p, T, dims; assignment)
Base.randn(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    randn(p, Float64, dims; assignment)
Base.randn(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    randn(p, Float64, dims; assignment)
Base.randn(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    randn(auto_blocks(dims), T, dims; assignment)

function sprand(p::Blocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, (T, _dims) -> sprand(T, _dims..., sparsity), false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
sprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    sprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
sprand(p::BlocksOrAuto, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    sprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
sprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    sprand(p, Float64, dims, sparsity; assignment)
sprand(::AutoBlocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    sprand(auto_blocks(dims), T, dims, sparsity; assignment)

function Base.ones(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, ones, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.ones(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    ones(p, T, dims; assignment)
Base.ones(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    ones(p, Float64, dims; assignment)
Base.ones(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    ones(p, Float64, dims; assignment)
Base.ones(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    ones(auto_blocks(dims), T, dims; assignment)

function Base.zeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, zeros, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.zeros(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    zeros(p, T, dims; assignment)
Base.zeros(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    zeros(p, Float64, dims; assignment)
Base.zeros(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    zeros(p, Float64, dims; assignment)
Base.zeros(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    zeros(auto_blocks(dims), T, dims; assignment)

function Base.zero(x::DArray{T,N}) where {T,N}
    dims = ntuple(i->x.domain.indexes[i].stop, N)
    sd = first(x.subdomains)
    part_size = ntuple(i->sd.indexes[i].stop, N)
    a = zeros(Blocks(part_size...), T, dims)
    return _to_darray(a)
end

# Weird LinearAlgebra dispatch in `\` needs this
function LinearAlgebra._zeros(::Type{T}, B::DVector, n::Integer) where T
    m = max(size(B, 1), n)
    sz = (m,)
    return zeros(auto_blocks(sz), T, sz)
end
function LinearAlgebra._zeros(::Type{T}, B::DMatrix, n::Integer) where T
    m = max(size(B, 1), n)
    sz = (m, size(B, 2))
    return zeros(auto_blocks(sz), T, sz)
end

function Base.view(A::AbstractArray{T,N}, p::Blocks{N}) where {T,N}
    d = ArrayDomain(Base.index_shape(A))
    dc = partition(p, d)
    # N.B. We use `tochunk` because we only want to take the view locally, and
    # taking views should be very fast
    chunks = [tochunk(view(A, x.indexes...)) for x in dc]
    return DArray(T, d, dc, chunks, p)
end
Base.view(A::AbstractArray, ::AutoBlocks) =
    view(A, auto_blocks(size(A)))

function unsafe_free!(A::DArray)
    spawn_datadeps() do
        for chunk in A.chunks
            scope = UnionScope(map(ExactScope, collect(processors(memory_space(chunk)))))
            Dagger.@spawn scope=scope unsafe_free!(chunk)
        end
    end
end
