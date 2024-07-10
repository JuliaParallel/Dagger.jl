export spmd_rank, spmd_size, spmd, spmd_exchange, spmd_exchange!, spmd_reduce, spmd_reduce!

const SPMD_INIT = TaskLocalValue{Bool}(()->false)
const SPMD_RANK = TaskLocalValue{Int}(()->0)
const SPMD_SIZE = TaskLocalValue{Int}(()->1)
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

function spmd(f, nranks::Integer, data...; parallelize=:threads)
    tasks = Vector{DTask}(undef, nranks)
    all_chans = [i != j ? RemoteChannel() : nothing for i in 1:nranks, j in 1:nranks]
    @sync for rank in 1:nranks
        data_split = map(data) do d
            if d isa Ref
                return d[]
            else
                @assert length(d) == nranks
                return d[rank]
            end
        end
        if parallelize == :workers
            tasks[rank] = Dagger.@spawn scope=Dagger.scope(worker=rank, thread=1) _spmd(f, rank, nranks, all_chans, data_split...)
        elseif parallelize == :threads
            tasks[rank] = Dagger.@spawn scope=Dagger.scope(worker=1, thread=rank) _spmd(f, rank, nranks, all_chans, data_split...)
        else
            error("Invalid parallelization strategy: $parallelize")
        end
    end
    return map(fetch, tasks)
end
function _spmd(f, rank, nranks, all_chans, data...)
    SPMD_INIT[] = true
    SPMD_RANK[] = rank
    SPMD_SIZE[] = nranks
    SPMD_ALL_CHANNELS[] = all_chans

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

# Collectives

function spmd_exchange(value::T) where T
    V = Vector{Chunk{T}}(undef, spmd_size())
    spmd_exchange!(value, V)
    return V
end
function spmd_exchange!(value::T, V::Vector{Chunk{T}}) where T
    rank = spmd_rank()
    value_chunk = Dagger.tochunk(value)
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
            V[other_rank] = take!(SPMD_ALL_CHANNELS[][other_rank, rank])
        end
    end

    return V
end

spmd_barrier() = spmd_exchange(0)

#= FIXME
function spmd_reduce(op, value::T) where T
    #V = spmd_exchange(value)
    #return reduce(op, V)
    V = spmd_exchange(value)
    if spmd_rank() == 1
        DV = distribute(V, Dagger.ParallelBlocks())
        allreduce!(op, DV)
    end
    return V[spmd_rank()]
end
=#
function spmd_reduce!(op, value::T) where T
    V = spmd_exchange(value)
    if spmd_rank() == 1
        DV = distribute_all(V, Dagger.ParallelBlocks{ndims(value)}(spmd_size()))
        allreduce!(op, DV)
    end
    spmd_barrier()
    return
end
