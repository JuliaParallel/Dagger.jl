export spmd_rank, spmd_size, spmd, spmd_exchange, spmd_exchange!, spmd_reduce, spmd_reduce!

const SPMD_INIT = TaskLocalValue{Bool}(()->false)
const SPMD_RANK = TaskLocalValue{Int}(()->0)
const SPMD_SIZE = TaskLocalValue{Int}(()->1)
const SPMD_SCOPE = TaskLocalValue{AbstractScope}(()->AnyScope())
const SPMD_ALL_CHANNELS = TaskLocalValue{Matrix{Union{RemoteChannel,Nothing}}}(()->Matrix{RemoteChannel}(undef, 1, 1))

function check_spmd_init()
    if !SPMD_INIT[]
        throw(ConcurrencyViolationError("SPMD region has not been configured\nPlease use spmd() to start a region"))
    end
end
function spmd_rank()
    check_spmd_init()
    return SPMD_RANK[]
end
function spmd_size()
    check_spmd_init()
    return SPMD_SIZE[]
end
function spmd_scope()
    check_spmd_init()
    return SPMD_SCOPE[]
end

function spmd(f, nranks::Integer, data...;
              scope::AbstractScope=get_options(:scope, DefaultScope()),
              parallelize::Union{Symbol,Nothing}=nothing)
    # Get all valid processors
    if parallelize === nothing
        all_procs = collect(compatible_processors(scope))
    elseif parallelize == :workers
        all_procs = filter(proc->proc isa ThreadProc && proc.tid == 1, all_processors())
    elseif parallelize == :threads
        all_procs = filter(proc->proc isa ThreadProc && proc.owner == 1, all_processors())
    else
        throw(ArgumentError("Invalid parallelization strategy: $parallelize\nValid options are :workers and :threads, or nothing"))
    end
    if length(all_procs) < nranks
        throw(ArgumentError("Not enough processors to run $nranks ranks"))
    end
    new_scope = UnionScope(map(ExactScope, all_procs[1:nranks]))

    # Allocate DTasks and RemoteChannels
    tasks = Vector{DTask}(undef, nranks)
    all_chans = [i != j ? RemoteChannel() : nothing for i in 1:nranks, j in 1:nranks]

    # Launch tasks
    @sync for (rank, proc) in zip(1:nranks, all_procs)
        data_split = map(data) do d
            if d isa Ref
                return d[]
            else
                @assert length(d) == nranks
                return d[rank]
            end
        end
        tasks[rank] = Dagger.@spawn scope=ExactScope(proc) _spmd_exec(f, rank, nranks, all_chans, new_scope, data_split...)
    end

    return tasks
end
function _spmd_exec(f, rank, nranks, all_chans, scope, data...)
    SPMD_INIT[] = true
    SPMD_RANK[] = rank
    SPMD_SIZE[] = nranks
    SPMD_ALL_CHANNELS[] = all_chans
    SPMD_SCOPE[] = scope

    result = nothing
    try
        return f(data...)
    finally
        for chan in SPMD_ALL_CHANNELS[][rank, :]
            chan === nothing && continue
            close(chan)
        end
    end
end

@warn "Add tags for P2P and collectives" maxlog=1

# Peer-to-Peer

function spmd_send(rank::Integer, value)
    check_spmd_init()
    chan = SPMD_ALL_CHANNELS[][spmd_rank(), rank]
    put!(chan, value)
    return
end
function spmd_recv(rank::Integer)
    check_spmd_init()
    chan = SPMD_ALL_CHANNELS[][rank, spmd_rank()]
    return take!(chan)
end

# Collectives

function spmd_exchange(value::T) where T
    V = Vector{Chunk{T}}(undef, spmd_size())
    spmd_exchange!(value, V)
    return V
end
function spmd_exchange!(value::T, V::Vector{Chunk{T}}) where T
    rank = spmd_rank()
    space = memory_space(value)
    # TODO: Pass space directly
    proc = first(processors(space))
    value_chunk = Dagger.tochunk(value, proc)
    V[rank] = value_chunk

    # Send our value to them
    for other_rank in 1:spmd_size()
        if other_rank != rank
            put!(SPMD_ALL_CHANNELS[][rank, other_rank], value_chunk)
        end
    end

    # Receive their value
    for other_rank in 1:spmd_size()
        if other_rank != rank
            V[other_rank] = Sch.thunk_yield() do
                take!(SPMD_ALL_CHANNELS[][other_rank, rank])
            end
        end
    end

    return V
end

spmd_barrier() = spmd_exchange(0)

function spmd_reduce(op, value::AbstractArray; kwargs...)
    spmd_reduce!(op, value; kwargs...)
    return value
end
function spmd_reduce!(op, value::AbstractArray; kwargs...)
    Dvalue = view(value, Blocks(size(value)...))
    Dvalues = spmd_exchange(Dvalue)
    if spmd_rank() == 1
        # Only one rank needs to schedule the reduction
        Dagger.with_options(;scope=spmd_scope()) do
            allreduce!(op, Dvalues; kwargs...)
        end
    end
    spmd_barrier()
    return
end
function allreduce!(op::Function, xs::Vector{<:DArray};
                    num_splits::Integer = 1,
                    split_dim::Integer = ndims(xs[1]))
    # Split each chunk along the selected dimension
    x1 = first(xs)::DArray
    chunk_size = cld(size(x1, split_dim), num_splits)
    chunk_dist = Blocks(ntuple(i->i == split_dim ? chunk_size : size(x, i), N))
    chunk_ds = partition(chunk_dist, x.subdomains[1])
    num_par_chunks = length(xs)

    # Allocate temporary buffer
    ys = map(copy, xs)

    # Ring-reduce into temporary buffer
    Dagger.spawn_datadeps() do
        for j in 1:length(chunk_ds)
            for i in 1:num_par_chunks
                for step in 1:(num_par_chunks-1)
                    from_idx = i
                    to_idx = mod1(i+step, num_par_chunks)
                    from_chunk = xs[from_idx]
                    to_chunk = ys[to_idx]
                    sd = chunk_ds[mod1(j+i-1, length(chunk_ds))].indexes
                    # FIXME: Specify aliasing based on `sd`
                    Dagger.@spawn _reduce_view!(op,
                                                InOut(to_chunk), sd,
                                                In(from_chunk), sd)
                end
            end
        end

        # Copy from temporary buffer back to origin
        for i in 1:num_par_chunks
            Dagger.@spawn copyto!(Out(xs[i]), In(ys[i]))
        end
    end

    return xs
end
function _reduce_view!(op, to, to_view, from, from_view)
    to_viewed = view(to, to_view...)
    from_viewed = view(from, from_view...)
    _reduce!(op, to_viewed, from_viewed)
    return
end
function _reduce!(op, to, from)
    to .= op.(to, from)
end
