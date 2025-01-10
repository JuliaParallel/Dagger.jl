# Mempool for received MPI message data only (no envelopes).
# Key: (comm, source, tag). Used when a message is received but not the one the caller was waiting for.
# Included from mpi.jl; runs in Dagger module scope.

const MPI_RECV_MEMPOOL = Base.Lockable(Dict{Tuple{MPI.Comm, Int, Int}, Vector{Any}}())

function mpi_mempool_put!(comm::MPI.Comm, source::Integer, tag::Integer, data::Any)
    key = (comm, Int(source), Int(tag))
    ref = poolset(data)
    lock(MPI_RECV_MEMPOOL) do pool
        if !haskey(pool, key)
            pool[key] = Any[]
        end
        push!(pool[key], ref)
    end
    return nothing
end

function mpi_mempool_take!(comm::MPI.Comm, source::Integer, tag::Integer)
    key = (comm, Int(source), Int(tag))
    ref = lock(MPI_RECV_MEMPOOL) do pool
        if !haskey(pool, key) || isempty(pool[key])
            return nothing
        end
        popfirst!(pool[key])
    end
    ref === nothing && return nothing
    return poolget(ref)
end

function mpi_mempool_has(comm::MPI.Comm, source::Integer, tag::Integer)
    key = (comm, Int(source), Int(tag))
    return lock(MPI_RECV_MEMPOOL) do pool
        haskey(pool, key) && !isempty(pool[key])
    end
end
