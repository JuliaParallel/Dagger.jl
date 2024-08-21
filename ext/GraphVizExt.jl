module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
else
    using ..GraphViz
end

import Dagger

"""
    render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                color_by=:fn, layout_engine="dot",
                times::Bool=true, times_digits::Integer=3,
                colors=Dagger.Viz.default_colors,
                name_to_color=Dagger.Viz.name_to_color)

Render a graph of the task dependencies and data dependencies in `logs` using GraphViz.

Requires the `all_task_deps` event enabled in `enable_logging!`

Options:
- `disconnected`: If `true`, render disconnected vertices (tasks or arguments without upstream/downstream dependencies)
- `color_by`: How to color tasks; if `:fn`, then color by unique function name, if `:proc`, then color by unique processor
- `layout_engine`: The layout engine to use for GraphViz rendering
- `times`: If `true`, annotate each task with its start and finish times
- `times_digits`: Number of digits to display in the time annotations
- `colors`: A list of colors to use for coloring tasks
- `name_to_color`: A function that maps task names to colors
"""
function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                            color_by=:fn, layout_engine="dot",
                            times::Bool=true, times_digits::Integer=3,
                            colors=Dagger.Viz.default_colors,
                            name_to_color=Dagger.Viz.name_to_color)
    dot = Dagger.Viz.logs_to_dot(logs; disconnected, times, times_digits,
                                 color_by, colors, name_to_color)
    gv = GraphViz.Graph(dot)
    GraphViz.layout!(gv; engine=layout_engine)
    return gv
end

end
