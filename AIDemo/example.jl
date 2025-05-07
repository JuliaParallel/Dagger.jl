#using Pkg; Pkg.add("Dagger")

using Distributed

addprocs(4; exeflags="--project=$(pwd())")

using Dagger

tasks = [Dagger.@spawn 1+i for i in 1:10]

tasks = [Dagger.@spawn (i->begin
    value = 1+i
    println("Task $i has value $value")
    return value
end)(i) for i in 1:10]

foreach(println, fetch.(tasks))

using Plots, DataFrames
using GraphViz
function with_plots(f)
    Dagger.enable_logging!(; metrics=false, all_task_deps=true)
    GC.enable(false)
    try
        f()
    finally
        GC.enable(true)
        logs = Dagger.fetch_logs!()
        Dagger.disable_logging!()
        display(Dagger.render_logs(logs, :plots_gantt; target=:execution, color_init_hash=UInt(1)))
        display(Dagger.render_logs(logs, :graphviz))
    end
end
macro with_plots(ex)
    quote
        with_plots(()->$(esc(ex)))
    end
end
