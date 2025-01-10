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
    procgrid::Union{AbstractArray{<:Processor, N}, Nothing}

    function AllocateArray(eltype::Type{T}, f, want_index::Bool, d::ArrayDomain{N}, domainchunks, p::AbstractBlocks{N}, assignment::Union{AssignmentType{N},Nothing} = nothing) where {T,N}
        sizeA = map(length, d.indexes)
        procgrid = nothing
        availprocs = collect(Dagger.compatible_processors())
        if !(assignment isa AbstractArray{<:Processor, N})
            filter!(p -> p isa ThreadProc, availprocs)
            sort!(availprocs, by = x -> (x.owner, x.tid))
        end
        np = length(availprocs)
        if assignment isa Symbol
            if assignment == :arbitrary
                procgrid = nothing
            elseif assignment == :blockrow
                q = ntuple(i -> i == 1 ? Int(ceil(sizeA[1] / p.blocksize[1])) : 1, N)
                rows_per_proc, extra = divrem(Int(ceil(sizeA[1] / p.blocksize[1])), np)
                counts = [rows_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
                procgrid = reshape(vcat(fill.(availprocs, counts)...), q)
            elseif assignment == :blockcol
                q = ntuple(i -> i == N ? Int(ceil(sizeA[N] / p.blocksize[N])) : 1, N)
                cols_per_proc, extra = divrem(Int(ceil(sizeA[N] / p.blocksize[N])), np)
                counts = [cols_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
                procgrid = reshape(vcat(fill.(availprocs, counts)...), q)
            elseif assignment == :cyclicrow
                q = ntuple(i -> i == 1 ? np : 1, N)
                procgrid = reshape(availprocs, q)
            elseif assignment == :cycliccol
                q = ntuple(i -> i == N ? np : 1, N)
                procgrid = reshape(availprocs, q)
            else
                error("Unsupported assignment symbol: $assignment, use :arbitrary, :blockrow, :blockcol, :cyclicrow or :cycliccol")
            end
        elseif assignment isa AbstractArray{<:Int, N}
            missingprocs = filter(q -> q ∉ procs(), assignment)
            isempty(missingprocs) || error("Specified workers are not available: $missingprocs")
            procgrid = [ThreadProc(proc, 1) for proc in assignment]
        elseif assignment isa AbstractArray{<:Processor, N}
            missingprocs = filter(q -> q ∉ availprocs, assignment)
            isempty(missingprocs) || error("Specified processors are not available: $missingprocs")
            procgrid = assignment
        end

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
    tasks = Array{DTask,ndims(A.domainchunks)}(undef, size(A.domainchunks)...)
    Dagger.spawn_datadeps() do
        default_scope = get_compute_scope()
        for I in CartesianIndices(A.domainchunks)
            x = A.domainchunks[I]
            i = LinearIndices(A.domainchunks)[I]

            if isnothing(A.procgrid)
                scope = default_scope
            else
                scope = ExactScope(A.procgrid[CartesianIndex(mod1.(Tuple(I), size(A.procgrid))...)])
            end

            if A.want_index
                task = Dagger.@spawn compute_scope=scope allocate_array(A.f, A.eltype, i, size(x))
            else
                task = Dagger.@spawn compute_scope=scope allocate_array(A.f, A.eltype, size(x))
            end
            tasks[i] = task
        end
    end
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

@warn "Consider a better way to provide a unique ID for each chunk" maxlog=1
function Base.view(A::AbstractArray{T,N}, p::Blocks{N}; space=default_memory_space(current_acceleration(), A)) where {T,N}
    d = ArrayDomain(Base.index_shape(A))
    dc = partition(p, d)
    # N.B. We use `tochunk` because we only want to take the view locally, and
    # taking views should be very fast
    chunks = [@with(MPI_UID => eager_next_id(), tochunk(view(A, x.indexes...), space)) for x in dc]
    return DArray(T, d, dc, chunks, p)
end
