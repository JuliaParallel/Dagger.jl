# Scope-Processor helpers

"""
    get_compute_scope() -> AbstractScope

Returns the currently set compute scope, first checking the `compute_scope`
option, then checking the `scope` option, and finally defaulting to
`DefaultScope()`.
"""
function get_compute_scope()
    opts = get_options()
    if hasproperty(opts, :compute_scope)
        return opts.compute_scope
    elseif hasproperty(opts, :scope)
        return opts.scope
    else
        return DefaultScope()
    end
end

"""
    compatible_processors(scope::AbstractScope, ctx::Context=Sch.eager_context()) -> Set{Processor}

Returns the set of all processors (across all Distributed workers) that are
compatible with the given scope.
"""
compatible_processors(scope::AbstractScope=get_compute_scope(), ctx::Context=Sch.eager_context()) =
    compatible_processors(scope, procs(ctx))
# Prefer the OSProc specialization (Context.procs / procs_to_use); the
# Vector{<:Processor} method remains for callers that pass a mixed list.
#
# Cache keyed by (objectid(scope), procs identity). Scopes and the worker
# list change rarely relative to task rate; rebuilding the Set{Processor}
# on every schedule_one! was a major allocation source.
const COMPAT_PROCS_CACHE = LockedObject(Dict{Tuple{UInt,UInt},Set{Processor}}())
const COMPAT_PROCS_CACHE_GEN = Threads.Atomic{UInt}(UInt(0))

"Invalidate the compatible_processors cache (call after addprocs!/rmprocs!)."
function invalidate_compatible_processors_cache!()
    Threads.atomic_add!(COMPAT_PROCS_CACHE_GEN, UInt(1))
    lock(COMPAT_PROCS_CACHE) do cache
        empty!(cache)
    end
    return
end

function compatible_processors(scope::AbstractScope, procs::Vector{OSProc})
    gen = COMPAT_PROCS_CACHE_GEN[]
    key = (objectid(scope), hash(procs, gen))
    cached = lock(COMPAT_PROCS_CACHE) do cache
        get(cache, key, nothing)
    end
    cached !== nothing && return cached
    compat_procs = Set{Processor}()
    for gproc in procs
        # Fast-path in case entire process is incompatible
        gproc_scope = ProcessScope(gproc)
        if !isa(constrain(scope, gproc_scope), InvalidScope)
            for proc in get_processors(gproc)
                if proc_in_scope(proc, scope)
                    push!(compat_procs, proc)
                end
            end
        end
    end
    lock(COMPAT_PROCS_CACHE) do cache
        # Bound cache size to avoid unbounded growth with unique scopes
        if length(cache) >= 256
            empty!(cache)
        end
        cache[key] = compat_procs
    end
    return compat_procs
end
function compatible_processors(scope::AbstractScope, procs::Vector{<:Processor})
    # Fall back: coerce to OSProc list (throws if non-OSProc present)
    return compatible_processors(scope, OSProc[p::OSProc for p in procs])
end

"""
    num_processors(scope::AbstractScope=DefaultScope(), all::Bool=false) -> Int

Returns the number of processors available to Dagger by default, or if
specified, according to `scope`. If `all=true`, instead returns the number of
processors known to Dagger, whether or not they've been disabled by the user.
Most users will want to use `num_processors()`.
"""
function num_processors(scope::AbstractScope=get_compute_scope();
                        all::Bool=false)
    if all
        return length(all_processors())
    else
        return length(compatible_processors(scope))
    end
end
