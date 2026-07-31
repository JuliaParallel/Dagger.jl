# Processor utilities

"""
    all_processors(ctx::Context=Sch.eager_context()) -> Set{Processor}

Returns the set of all processors available to the scheduler, across all
Distributed workers.
"""
function all_processors(ctx::Context=Sch.eager_context())
    all_procs = Set{Processor}()
    for gproc in procs(ctx)
        for proc in get_processors(gproc)
            push!(all_procs, proc)
        end
    end
    return all_procs
end
