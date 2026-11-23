using TimespanLogging

struct Renderer
    logs::Dict{Symbol,Vector{Any}}
    custom_plots::Vector{Base.Callable}
end
Renderer() = Renderer(Dict{Symbol,Vector{Any}}(), Base.Callable[])
Base.push!(r::Renderer, f::Base.Callable) =
    push!(r.custom_plots, f)

function TimespanLogging.Events.creation_hook(r::Renderer, log)
    for id in keys(log)
        log_vec = get!(r.logs, id) do
            []
        end
        push!(log_vec, log[id])
    end
end
function TimespanLogging.Events.deletion_hook(r::Renderer, idx)
    for id in keys(r.logs)
        deleteat!(r.logs[id], 1:idx)
    end
end
