function render_saturation!(logs, key::Symbol)
    sat = logs[key]
    if length(sat) == 0
        return
    end
    all_keys = unique(Iterators.flatten(if sat[1] isa NamedTuple
        map(ks->keys(ks), sat)
    elseif sat[1] isa Vector
        map(ks->map(first, ks), sat)
    end))
    sort!(all_keys; by=x->repr(x))
    for key in all_keys
        vec = if sat[1] isa NamedTuple
            map(e->Float32(get(e, key, 0)), sat)
        elseif sat[1] isa Vector
            map(e->Float32(last(get(e, something(findfirst(x->first(x) == key, e), 0), (key, 0)))), sat)
        end
        str = @sprintf("%0.2f %s", Float64(sum(vec)/length(vec)), key)
        CImGui.PlotLines(str, vec, length(vec))
    end
end
function inner_gui(r::Renderer)
    # Render event saturation
    render_saturation!(r.logs, :esat)

    CImGui.Separator()

    # Render processor saturation
    render_saturation!(r.logs, :psat)

    CImGui.Separator()

    # Custom rendering
    for (idx,f) in enumerate(r.custom_plots)
        f(r.logs)
        if idx < length(r.custom_plots)
            CImGui.Separator()
        end
    end
end
