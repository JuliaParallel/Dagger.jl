module ProfileCanvasExt

if isdefined(Base, :get_extension)
    using ProfileCanvas
else
    using ..ProfileCanvas
end

import Dagger

function Dagger.render_logs(logs::Dict, ::Val{:profile_canvas};
                            profile_name::Symbol=:profile, kwargs...)
    @assert any(w->haskey(logs[w], profile_name), keys(logs)) "Profile stream not found with name: $(repr(profile_name))"
    views = Dict{Any, ProfileCanvas.ProfileData}()
    for w in keys(logs)
        for idx in 1:length(logs[w][profile_name])
            frames = logs[w][profile_name][idx]
            if frames !== nothing
                view = ProfileCanvas.view(frames; kwargs...)
                category = logs[w][:core][idx].category
                id = logs[w][:id][idx]
                views[(w, category, id)] = view
            end
        end
    end
    return views
end

end # module ProfileCanvasExt
