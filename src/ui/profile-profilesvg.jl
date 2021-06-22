import .ProfileSVG

function prof_to_svg(path::String, pr, lidict, image_idx; width=1000)
    length(pr) > 0 || return
    if isdir(path)
        path = joinpath(path, repr(image_idx) * ".svg")
    end
    open(path, "w") do io
        ProfileSVG.save(io, pr; lidict=lidict, width=width)
    end
end
