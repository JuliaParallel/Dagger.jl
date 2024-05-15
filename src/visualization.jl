# Printable representation

"""
    show_logs(io::IO, logs, vizmode::Symbol; options...)
    show_logs(io::IO, t, logs, vizmode::Symbol; options...)

Displays the logs of a task `t` and/or logs object `logs` using the visualization mode `vizmode`, which is written to the given IO stream `io`. `options` are specific to the visualization mode.

---

    show_logs(logs, vizmode::Symbol; options...)
    show_logs(t, logs, vizmode::Symbol; options...)

Returns a string representation of the logs of a task `t` and/or logs object `logs` using the visualization mode `vizmode`. `options` are specific to the visualization mode.
"""
function show_logs end

show_logs(io::IO, logs, vizmode::Symbol; options...) =
    show_logs(io, logs, Val{vizmode}(); options...)
show_logs(io::IO, t, logs, vizmode::Symbol; options...) =
    show_logs(io, t, Val{vizmode}(); options...)
show_logs(io::IO, ::T, ::Val{vizmode}; options...) where {T,vizmode} =
    throw(ArgumentError("show_logs: Task/logs type `$T` not supported for visualization mode `$(repr(vizmode))`"))
show_logs(io::IO, ::T, ::Logs, ::Val{vizmode}; options...) where {T,Logs,vizmode} =
    throw(ArgumentError("show_logs: Task type `$T` and logs type `$Logs` not supported for visualization mode `$(repr(vizmode))`"))

show_logs(logs, vizmode::Symbol; options...) =
    show_logs(logs, Val{vizmode}(); options...)
show_logs(t, logs, vizmode::Symbol; options...) =
    show_logs(t, logs, Val{vizmode}(); options...)
function show_logs(logs, ::Val{vizmode}; options...) where vizmode
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

"""
    render_logs(logs, vizmode::Symbol; options...)
    render_logs(t, logs, vizmode::Symbol; options...)

Returns a displayable representation of the logs of a task `t` and/or logs object `logs` using the visualization mode `vizmode`. `options` are specific to the visualization mode.
"""
function render_logs end

render_logs(logs, vizmode::Symbol; options...) =
    render_logs(logs, Val{vizmode}(); options...)
render_logs(t, logs, vizmode::Symbol; options...) =
    render_logs(t, logs, Val{vizmode}(); options...)
render_logs(::T, ::Val{vizmode}; options...) where {T,vizmode} =
    throw(ArgumentError("render_logs: Task/logs type `$T` not supported for visualization mode `$(repr(vizmode))`"))
render_logs(::T, ::Logs, ::Val{vizmode}; options...) where {T,Logs,vizmode} =
    throw(ArgumentError("render_logs: Task type `$T` and logs type `$Logs` not supported for visualization mode `$(repr(vizmode))`"))
