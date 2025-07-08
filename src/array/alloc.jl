import Base: cat
import Random: MersenneTwister
export partition

mutable struct AllocateArray{T,N} <: ArrayOp{T,N}
    eltype::Type{T}
    f
    want_index::Bool
    domain::ArrayDomain{N}
    domainchunks
    partitioning::AbstractBlocks{N}
    procgrid::Union{AbstractArray{<:Processor, N}, Nothing}
end
size(a::AllocateArray) = size(a.domain)

function AllocateArray(eltype::Type{T}, f, want_index::Bool, d::ArrayDomain{N}, domainchunks, p::AbstractBlocks{N}, assignment::AssignmentType{N}) where {T,N}
    sizeA = map(length, d.indexes)
    procgrid = nothing
    availprocs = collect(Dagger.all_processors())
    sort!(availprocs, by = x -> (x.owner, x.tid))
    if assignment isa Symbol
        if assignment == :arbitrary
            procgrid = nothing
        elseif assignment == :blockrow
            q = ntuple(i -> i == 1 ? Int(ceil(sizeA[1] / p.blocksize[1])) : 1, N)
            rows_per_proc, extra = divrem(Int(ceil(sizeA[1] / p.blocksize[1])), num_processors())
            counts = [rows_per_proc + (i <= extra ? 1 : 0) for i in 1:num_processors()]
            procgrid = reshape(vcat(fill.(availprocs, counts)...), q)
        elseif assignment == :blockcol
            q = ntuple(i -> i == N ? Int(ceil(sizeA[N] / p.blocksize[N])) : 1, N)
            cols_per_proc, extra = divrem(Int(ceil(sizeA[N] / p.blocksize[N])), num_processors())
            counts = [cols_per_proc + (i <= extra ? 1 : 0) for i in 1:num_processors()]
            procgrid = reshape(vcat(fill.(availprocs, counts)...), q)
        elseif assignment == :cyclicrow
            q = ntuple(i -> i == 1 ? num_processors() : 1, N)
            procgrid = reshape(availprocs, q)
        elseif assignment == :cycliccol
            q = ntuple(i -> i == N ? num_processors() : 1, N)
            procgrid = reshape(availprocs, q)
        else
            error("Unsupported assignment symbol: $assignment, use :arbitrary, :blockrow, :blockcol, :cyclicrow or :cycliccol")
        end
    elseif assignment isa AbstractArray{<:Int, N}
        missingprocs = filter(q -> q ∉ procs(), assignment)
        isempty(missingprocs) || error("Missing processors: $missingprocs")
        procgrid = [Dagger.ThreadProc(proc, 1) for proc in assignment]
    elseif assignment isa AbstractArray{<:Processor, N}
        missingprocs = filter(q -> q ∉ availprocs, assignment)
        isempty(missingprocs) || error("Missing processors: $missingprocs")
        procgrid = assignment
    end

    return AllocateArray{T,N}(eltype, f, want_index, d, domainchunks, p, procgrid)
end

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
function stage(ctx, a::AllocateArray)
    chunks = map(CartesianIndices(a.domainchunks)) do I
        x = a.domainchunks[I]
        i = LinearIndices(a.domainchunks)[I]
        args = a.want_index ? (i, size(x)) : (size(x),)
        scope = isnothing(a.procgrid) ? nothing : ExactScope(a.procgrid[CartesianIndex(mod1.(Tuple(I), size(a.procgrid))...)])

        if isnothing(scope) 
            Dagger.@spawn allocate_array(a.f, a.eltype, args...)
        else
            Dagger.@spawn compute_scope=scope allocate_array(a.f, a.eltype, args...)
        end
    end
    return DArray(a.eltype, a.domain, a.domainchunks, chunks, a.partitioning)
end

const BlocksOrAuto = Union{Blocks{N} where N, AutoBlocks}

function Base.rand(p::Blocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, rand, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.rand(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) = rand(p, T, dims; assignment=assignment)
Base.rand(p::BlocksOrAuto, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) = rand(p, T, dims; assignment=assignment)
Base.rand(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) = rand(p, Float64, dims; assignment=assignment)
Base.rand(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) = rand(p, Float64, dims; assignment=assignment)
Base.rand(::AutoBlocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    rand(auto_blocks(dims), eltype, dims; assignment=assignment)

function Base.randn(p::Blocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, randn, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.randn(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) = randn(p, T, dims; assignment=assignment)
Base.randn(p::BlocksOrAuto, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) = randn(p, T, dims; assignment=assignment)
Base.randn(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) = randn(p, Float64, dims; assignment=assignment)
Base.randn(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) = randn(p, Float64, dims; assignment=assignment)
Base.randn(::AutoBlocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    randn(auto_blocks(dims), eltype, dims; assignment=assignment)

function sprand(p::Blocks, eltype::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, (T, _dims) -> sprand(T, _dims..., sparsity), false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
sprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    sprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment=assignment)
sprand(p::BlocksOrAuto, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    sprand(p, T, dims, sparsity; assignment=assignment)
sprand(p::BlocksOrAuto, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    sprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment=assignment)
sprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    sprand(p, Float64, dims, sparsity; assignment=assignment)
sprand(::AutoBlocks, eltype::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    sprand(auto_blocks(dims), eltype, dims, sparsity; assignment=assignment)

function Base.ones(p::Blocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, ones, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.ones(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) = ones(p, T, dims; assignment=assignment)
Base.ones(p::BlocksOrAuto, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) = ones(p, T, dims; assignment=assignment)
Base.ones(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) = ones(p, Float64, dims; assignment=assignment)
Base.ones(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) = ones(p, Float64, dims; assignment=assignment)
Base.ones(::AutoBlocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    ones(auto_blocks(dims), eltype, dims; assignment=assignment)

function Base.zeros(p::Blocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(eltype, zeros, false, d, partition(p, d), p, assignment)
    return _to_darray(a)
end
Base.zeros(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) = zeros(p, T, dims; assignment=assignment)
Base.zeros(p::BlocksOrAuto, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) = zeros(p, T, dims; assignment=assignment)
Base.zeros(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) = zeros(p, Float64, dims; assignment=assignment)
Base.zeros(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) = zeros(p, Float64, dims; assignment=assignment)
Base.zeros(::AutoBlocks, eltype::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    zeros(auto_blocks(dims), eltype, dims; assignment=assignment)

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
