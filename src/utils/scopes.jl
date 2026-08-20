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
"""
    scope_processors(scope) -> Union{Nothing,Vector{Processor}}

The processors a scope names outright, or `nothing` if it does not name any (and
so has to be tested against each known processor instead).

A scope built from `ExactScope`s carries its own answer, which lets
`compatible_processors` skip the scan over every processor in the cluster. That
scan is per task, so it is what makes task placement cost grow with the size of
the cluster -- for the pinned scopes Datadeps hands the scheduler, entirely
needlessly.
"""
scope_processors(::AbstractScope) = nothing
scope_processors(scope::ExactScope) = Processor[scope.processor]
function scope_processors(scope::UnionScope)
    procs = Processor[]
    for inner in scope.scopes
        inner_procs = scope_processors(inner)
        inner_procs === nothing && return nothing
        append!(procs, inner_procs)
    end
    return procs
end

function compatible_processors(scope::AbstractScope, procs::Vector{<:Processor})
    named = scope_processors(scope)
    if named !== nothing
        compat_procs = Set{Processor}()
        for proc in named
            gproc = get_parent(proc)
            gproc in procs || continue
            # A named processor still has to exist and be enabled
            proc in get_processors(gproc) || continue
            push!(compat_procs, proc)
        end
        return compat_procs
    end
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
