# Scope-Processor helpers

"""
    compatible_processors(scope::AbstractScope, ctx::Context=Sch.eager_context()) -> Set{Processor}

Returns the set of all processors (across all Distributed workers) that are
compatible with the given scope.
"""
function compatible_processors(scope::AbstractScope=get_options(:scope, DefaultScope()), ctx::Context=Sch.eager_context())
    compat_procs = Set{Processor}()
    for gproc in procs(ctx)
        # Fast-path in case entire process is incompatible
        gproc_scope = ProcessScope(gproc)
        if !isa(constrain(scope, gproc_scope), InvalidScope)
            for proc in get_processors(gproc)
                proc_scope = ExactScope(proc)
                if !isa(constrain(scope, proc_scope), InvalidScope)
                    push!(compat_procs, proc)
                end
            end
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
function num_processors(scope::AbstractScope=get_options(:scope, DefaultScope());
                        all::Bool=false)
    if all
        return length(all_processors())
    else
        return length(compatible_processors(scope))
    end
end
