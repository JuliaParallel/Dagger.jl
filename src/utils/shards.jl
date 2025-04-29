"""
Maps a value to one of multiple distributed "mirror" values automatically when
used as a thunk argument. Construct using `@shard` or `shard`.
"""
struct Shard
    chunks::Dict{Processor,Chunk}
end

"""
    shard(f; kwargs...) -> Chunk{Shard}

Executes `f` on all workers in `workers`, wrapping the result in a
process-scoped `Chunk`, and constructs a `Chunk{Shard}` containing all of these
`Chunk`s on the current worker.

Keyword arguments:
- `procs` -- The list of processors to create pieces on. May be any iterable container of `Processor`s.
- `workers` -- The list of workers to create pieces on. May be any iterable container of `Integer`s.
- `per_thread::Bool=false` -- If `true`, creates a piece per each thread, rather than a piece per each worker.
"""
function shard(@nospecialize(f); procs=nothing, workers=nothing, per_thread=false)
    if procs === nothing
        if workers !== nothing
            procs = [OSProc(w) for w in workers]
        else
            procs = lock(Sch.eager_context()) do
                copy(Sch.eager_context().procs)
            end
        end
        if per_thread
            _procs = ThreadProc[]
            for p in procs
                append!(_procs, filter(p->p isa ThreadProc, get_processors(p)))
            end
            procs = _procs
        end
    else
        if workers !== nothing
            throw(ArgumentError("Cannot combine `procs` and `workers`"))
        elseif per_thread
            throw(ArgumentError("Cannot combine `procs` and `per_thread=true`"))
        end
    end
    isempty(procs) && throw(ArgumentError("Cannot create empty Shard"))
    shard_running_dict = Dict{Processor,DTask}()
    for proc in procs
        scope = proc isa OSProc ? ProcessScope(proc) : ExactScope(proc)
        thunk = Dagger.@spawn scope=scope _mutable_inner(f, proc, scope)
        shard_running_dict[proc] = thunk
    end
    shard_dict = Dict{Processor,Chunk}()
    for proc in procs
        shard_dict[proc] = fetch(shard_running_dict[proc])[]
    end
    return Shard(shard_dict)
end

"Creates a `Shard`. See [`Dagger.shard`](@ref) for details."
macro shard(exs...)
    opts = esc.(exs[1:end-1])
    ex = exs[end]
    quote
        let f = @noinline ()->$(esc(ex))
            $shard(f; $(opts...))
        end
    end
end

function move(from_proc::Processor, to_proc::Processor, shard::Shard)
    # Match either this proc or some ancestor
    # N.B. This behavior may bypass the piece's scope restriction
    proc = to_proc
    if haskey(shard.chunks, proc)
        return move(from_proc, to_proc, shard.chunks[proc])
    end
    parent = Dagger.get_parent(proc)
    while parent != proc
        proc = parent
        parent = Dagger.get_parent(proc)
        if haskey(shard.chunks, proc)
            return move(from_proc, to_proc, shard.chunks[proc])
        end
    end

    throw(KeyError(to_proc))
end
Base.iterate(s::Shard) = iterate(values(s.chunks))
Base.iterate(s::Shard, state) = iterate(values(s.chunks), state)
Base.length(s::Shard) = length(s.chunks)