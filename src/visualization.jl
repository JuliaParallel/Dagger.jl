# Printable representation

show_logs(io::IO, t, vizmode::Symbol; options...) =
    show_logs(io, t, Val{vizmode}(); options...)
show_logs(io::IO, t, logs, vizmode::Symbol; options...) =
    show_logs(io, t, Val{vizmode}(); options...)
show_logs(io::IO, ::T, ::Val{vizmode}; options...) where {T,vizmode} =
    throw(ArgumentError("show_logs: Task/logs type `$T` not supported for visualization mode `$(repr(vizmode))`"))
show_logs(io::IO, ::T, ::Logs, ::Val{vizmode}; options...) where {T,Logs,vizmode} =
    throw(ArgumentError("show_logs: Task type `$T` and logs type `$Logs` not supported for visualization mode `$(repr(vizmode))`"))

function show_logs(t, ::Val{vizmode}; options...) where vizmode
    iob = IOBuffer()
    show_logs(iob, t, Val{vizmode}(); options...)
    return String(take!(iob))
end
function show_logs(t, logs, ::Val{vizmode}; options...) where vizmode
    iob = IOBuffer()
    show_logs(iob, t, logs, Val{vizmode}(); options...)
    return String(take!(iob))
end

# Displayable representation

render_logs(t, vizmode::Symbol; options...) =
    render_logs(t, Val{vizmode}(); options...)
render_logs(t, logs, vizmode::Symbol; options...) =
    render_logs(t, Val{vizmode}(); options...)
render_logs(::T, ::Val{vizmode}; options...) where {T,vizmode} =
    throw(ArgumentError("render_logs: Task/logs type `$T` not supported for visualization mode `$(repr(vizmode))`"))
render_logs(::T, ::Logs, ::Val{vizmode}; options...) where {T,Logs,vizmode} =
    throw(ArgumentError("render_logs: Task type `$T` and logs type `$Logs` not supported for visualization mode `$(repr(vizmode))`"))
