module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
else
    using ..GraphViz
end

import Dagger

"""
    Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                       color_by=:fn, layout_engine="dot",
                       times::Bool=true, times_digits::Integer=3)

Render a graph of the task dependencies and data dependencies in `logs` using GraphViz.

Requires the following events enabled in `enable_logging!`: `taskdeps`, `tasknames`, `taskargs`, `taskargmoves`

Options:
- `disconnected`: If `true`, render disconnected vertices (tasks or arguments without upstream/downstream dependencies)
- `color_by`: How to color tasks; if `:fn`, then color by unique function name, if `:proc`, then color by unique processor
- `layout_engine`: The layout engine to use for GraphViz
- `times`: If `true`, annotate each task with its start and finish times
- `times_digits`: Number of digits to display in the time annotations
"""
function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                            color_by=:fn, layout_engine="dot",
                            times::Bool=true, times_digits::Integer=3)
    dot = Dagger.logs_to_dot(logs; disconnected=disconnected, color_by=color_by,
                             times=times, times_digits=times_digits)
    gv = GraphViz.Graph(dot)
    GraphViz.layout!(gv; engine=layout_engine)
    return gv
end

end
