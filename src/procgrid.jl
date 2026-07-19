# Compact processor grids for distributed array block assignment.

abstract type AbstractProcGrid{N} end

struct CyclicProcGrid{N} <: AbstractProcGrid{N}
    procs::Vector{Processor}
    shape::NTuple{N,Int}
end

struct BlockedProcGrid{N} <: AbstractProcGrid{N}
    procs::Vector{Processor}
    counts::Vector{Int}
    shape::NTuple{N,Int}
    dim::Int
end

struct DenseProcGrid{N} <: AbstractProcGrid{N}
    grid::Array{Processor,N}
end

DenseProcGrid(grid::AbstractArray{<:Processor,N}) where {N} =
    DenseProcGrid{N}(Array{Processor,N}(grid))

const AssignmentType{N} = Union{Symbol, AbstractArray{<:Int, N}, AbstractArray{<:Processor, N}}

procgrid_shape(pg::AbstractProcGrid{N}) where {N} = pg.shape
procgrid_shape(pg::DenseProcGrid{N}) where {N} = size(pg.grid)

function _tile_index(pg::AbstractProcGrid{N}, I::CartesianIndex{N}) where {N}
    shape = procgrid_shape(pg)
    return CartesianIndex(mod1.(Tuple(I), shape))
end

function procgrid_processor(pg::CyclicProcGrid{N}, I::CartesianIndex{N}) where {N}
    I_tiled = _tile_index(pg, I)
    lin = LinearIndices(pg.shape)[I_tiled]
    return pg.procs[mod1(lin, length(pg.procs))]
end

function procgrid_processor(pg::BlockedProcGrid{N}, I::CartesianIndex{N}) where {N}
    I_tiled = _tile_index(pg, I)
    idx = I_tiled[pg.dim]
    acc = 0
    for (p, c) in enumerate(pg.counts)
        acc += c
        if idx <= acc
            return pg.procs[p]
        end
    end
    return pg.procs[end]
end

procgrid_processor(pg::DenseProcGrid{N}, I::CartesianIndex{N}) where {N} =
    pg.grid[_tile_index(pg, I)]

function procgrid_scope(procgrid, I::CartesianIndex)
    if procgrid === nothing
        return get_compute_scope()
    end
    return ExactScope(procgrid_processor(procgrid, I))
end

normalize_procgrid(procgrid::Union{AbstractProcGrid, Nothing}) = procgrid
normalize_procgrid(procgrid::AbstractArray{<:Processor,N}) where {N} = DenseProcGrid(procgrid)

function build_procgrid(assignment::AssignmentType{N}, sizeA::NTuple{N,Int},
                        blocksize::NTuple{N,Int}, accel::Acceleration) where {N}
    # N.B. Collect with the abstract `Processor` eltype: the grid structs store
    # `Vector{Processor}` (invariant), so a concrete `Vector{ThreadProc}` from
    # `compatible_processors` would not match their constructors.
    availprocs = collect(Processor, compatible_processors(accel))
    if !(assignment isa AbstractArray{<:Processor, N})
        filter!(p -> p isa ThreadProc, availprocs)
        sort!(availprocs, by=x -> (x.owner, x.tid))
    end
    np = length(availprocs)
    nblocks = ntuple(i -> cld(sizeA[i], blocksize[i]), N)

    if assignment isa Symbol
        if assignment == :arbitrary
            return default_procgrid(accel, nblocks)
        elseif assignment == :blockrow
            shape = ntuple(i -> i == 1 ? Int(ceil(sizeA[1] / blocksize[1])) : 1, N)
            nrows = shape[1]
            rows_per_proc, extra = divrem(nrows, np)
            counts = [rows_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
            return BlockedProcGrid(availprocs, counts, shape, 1)
        elseif assignment == :blockcol
            shape = ntuple(i -> i == N ? Int(ceil(sizeA[N] / blocksize[N])) : 1, N)
            ncols = shape[N]
            cols_per_proc, extra = divrem(ncols, np)
            counts = [cols_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
            return BlockedProcGrid(availprocs, counts, shape, N)
        elseif assignment == :cyclicrow
            shape = ntuple(i -> i == 1 ? np : 1, N)
            return CyclicProcGrid(availprocs, shape)
        elseif assignment == :cycliccol
            shape = ntuple(i -> i == N ? np : 1, N)
            return CyclicProcGrid(availprocs, shape)
        else
            error("Unsupported assignment symbol: $assignment, use :arbitrary, :blockrow, :blockcol, :cyclicrow or :cycliccol")
        end
    elseif assignment isa AbstractArray{<:Int, N}
        missingprocs = filter(p -> p ∉ procs(), assignment)
        isempty(missingprocs) || error("Specified workers are not available: $missingprocs")
        return DenseProcGrid([ThreadProc(proc, 1) for proc in assignment])
    elseif assignment isa AbstractArray{<:Processor, N}
        missingprocs = filter(p -> p ∉ availprocs, assignment)
        isempty(missingprocs) || error("Specified processors are not available: $missingprocs")
        return DenseProcGrid(assignment)
    end
    error("Invalid assignment type: $(typeof(assignment))")
end

function emit_chunk_tasks!(domainchunks, procgrid, eltype::Type{T}, spawn_chunk::Function) where {T}
    N = ndims(domainchunks)
    tasks = Array{Any,N}(undef, size(domainchunks)...)
    default_scope = get_compute_scope()
    function emit!()
        for I in CartesianIndices(domainchunks)
            scope = procgrid === nothing ? default_scope : procgrid_scope(procgrid, I)
            i = LinearIndices(domainchunks)[I]
            tasks[i] = spawn_chunk(scope, I, i)
        end
    end
    # Under SPMD/MPI (uniform execution) the chunk-placement tasks must run
    # inside a datadeps region so every rank agrees on scheduling and data
    # movement. Under Distributed the placements are independent `@spawn`s with
    # explicit scopes; wrapping them in datadeps would needlessly require the
    # element type to be aliasing-resolvable (breaking e.g. non-isbits eltypes).
    if uniform_execution()
        spawn_datadeps(emit!)
    else
        emit!()
    end
    return post_stage_array_chunks!(current_acceleration(), tasks, eltype, N)
end

"Materialize a dense processor array (for tests and parity checks)."
function materialize_procgrid(procgrid::Union{AbstractProcGrid{N},Nothing}, nblocks::NTuple{N,Int}) where {N}
    procgrid === nothing && return nothing
    shape = ntuple(i -> max(nblocks[i], procgrid_shape(procgrid)[i]), N)
    grid = Array{Processor,N}(undef, shape)
    for I in CartesianIndices(grid)
        grid[I] = procgrid_processor(procgrid, I)
    end
    return grid
end

materialize_procgrid(procgrid::DenseProcGrid{N}, ::NTuple{N,Int}) where {N} = procgrid.grid
