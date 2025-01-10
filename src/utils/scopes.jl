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
function compatible_processors(scope::AbstractScope, procs::Vector{<:Processor})
    compat_procs = Set{Processor}()
    for gproc in procs
        for proc in get_processors(gproc)
            proc_in_scope(proc, scope) || continue
            push!(compat_procs, proc)
        end
    end
    return compat_procs
end
compatible_processors(acceleration::Acceleration, scope::AbstractScope=get_compute_scope(), ctx::Context=Sch.eager_context()) =
    compatible_processors(acceleration, scope, procs(ctx))
function compatible_processors(acceleration::Acceleration, scope::AbstractScope, procs::Vector{<:Processor})
    compat_procs = Set{Processor}()
    for gproc in procs
        accel_matches_proc(acceleration, gproc) || continue
        for proc in get_processors(gproc)
            accel_matches_proc(acceleration, proc) || continue
            proc_in_scope(proc, scope) || continue
            push!(compat_procs, proc)
        end
    end
    return compat_procs
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
