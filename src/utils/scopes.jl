# Scope-Processor helpers

"""
    compatible_processors(scope::AbstractScope, ctx::Context=Sch.eager_context()) -> Set{Processor}

Returns the set of all processors (across all Distributed workers) that are
compatible with the given scope.
"""
function compatible_processors(scope::AbstractScope, ctx::Context=Sch.eager_context())
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
