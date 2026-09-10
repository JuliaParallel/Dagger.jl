# Shared pitch visual language.
#
# Dark stage, one accent per physics, shared typeface and chrome. Every clip
# is 1920×1080, H.264, 10–20 s, loop-friendly (cross-fade the tail into the
# head). Titles + “solved with Dagger …” subtitles + residual / tile footer.

using CairoMakie
using ColorSchemes
using Colors
using LinearAlgebra

const PITCH_ROOT = normpath(joinpath(@__DIR__, ".."))
const VIDEO_DIR = joinpath(PITCH_ROOT, "videos")
const FRAME_DIR = joinpath(PITCH_ROOT, "frames")

const BG     = RGB(0.027, 0.039, 0.063)   # #070A10
const PANEL  = RGB(0.063, 0.078, 0.110)   # #10141C
const TEXT   = RGB(0.910, 0.929, 0.961)   # #E8EDF5
const MUTED  = RGB(0.545, 0.576, 0.655)   # #8B93A7
const RULE   = RGB(0.110, 0.137, 0.188)   # #1C2330
const DAGGER = RGB(0.780, 0.820, 0.890)

const ACCENT = (
    heat          = RGB(1.000, 0.420, 0.208),  # ember
    elasticity    = RGB(0.239, 0.863, 0.592),  # mint
    convection    = RGB(0.298, 0.788, 0.941),  # cyan
    multiphysics  = RGB(0.780, 0.490, 1.000),  # violet
    unstructured  = RGB(0.957, 0.827, 0.369),  # gold
    mixed         = RGB(0.482, 0.875, 0.949),  # ice
)

_cs(name::Symbol) = ColorSchemes.colorschemes[name]
const CMAP = (
    heat          = _cs(:inferno),
    elasticity    = _cs(:tokyo),
    convection    = _cs(:lipari),
    multiphysics  = _cs(:plasma),
    unstructured  = _cs(:viridis),
    mixed         = _cs(:ice),
)

const PART_COLORS = [
    RGB(0.957, 0.827, 0.369),
    RGB(0.298, 0.788, 0.941),
    RGB(0.780, 0.490, 1.000),
    RGB(0.239, 0.863, 0.592),
    RGB(1.000, 0.420, 0.208),
    RGB(0.482, 0.875, 0.949),
    RGB(0.95, 0.55, 0.70),
    RGB(0.55, 0.70, 0.95),
]

const WIDTH = 1920
const HEIGHT = 1080
const FPS = parse(Int, get(ENV, "PITCH_FPS", "24"))
const TARGET_SECS = parse(Int, get(ENV, "PITCH_SECS", "15"))
const NFRAMES = parse(Int, get(ENV, "PITCH_FRAMES", string(FPS * TARGET_SECS)))

function apply_pitch_theme!()
    set_theme!(Theme(
        figure_padding = (28, 28, 22, 22),
        backgroundcolor = BG,
        textcolor = TEXT,
        fontsize = 20,
        font = :regular,
        Axis = (
            backgroundcolor = PANEL,
            spinewidth = 1.0,
            leftspinecolor = RULE,
            rightspinecolor = RULE,
            topspinecolor = RULE,
            bottomspinecolor = RULE,
            xtickcolor = MUTED,
            ytickcolor = MUTED,
            xlabelcolor = MUTED,
            ylabelcolor = MUTED,
            xticklabelcolor = MUTED,
            yticklabelcolor = MUTED,
            titlecolor = TEXT,
            xgridcolor = (TEXT, 0.06),
            ygridcolor = (TEXT, 0.06),
            xgridvisible = false,
            ygridvisible = false,
        ),
        Axis3 = (
            backgroundcolor = PANEL,
            xspinesvisible = false,
            yspinesvisible = false,
            zspinesvisible = false,
            xticklabelcolor = MUTED,
            yticklabelcolor = MUTED,
            zticklabelcolor = MUTED,
            xlabelcolor = MUTED,
            ylabelcolor = MUTED,
            zlabelcolor = MUTED,
            titlecolor = TEXT,
        ),
        Colorbar = (
            ticklabelcolor = MUTED,
            labelcolor = MUTED,
            tickcolor = MUTED,
            spinecolor = RULE,
        ),
        Legend = (
            bgcolor = PANEL,
            framecolor = RULE,
            labelcolor = TEXT,
            titlecolor = MUTED,
        ),
    ))
    return nothing
end

"""
    pitch_chrome(accent; title, subtitle, footer...) -> (fig, main)

Shared 1080p chrome: accent hairline, title / Dagger wordmark, subtitle,
footer residual + tiles + clock. `main` is the content `GridLayout`.
"""
function pitch_chrome(accent::Colorant;
                      title::AbstractString,
                      subtitle::AbstractString,
                      footer_left::AbstractString = "",
                      footer_mid::AbstractString = "",
                      footer_right::AbstractString = "")
    fig = Figure(size = (WIDTH, HEIGHT), backgroundcolor = BG, fontsize = 20)
    root = fig.layout
    rowgap!(root, 8)
    colgap!(root, 12)

    # Accent hairline
    box = Box(fig[0, 1:3]; color = accent, strokewidth = 0)
    rowsize!(root, 0, Fixed(5))

    Label(fig[1, 1], uppercase(title);
          fontsize = 30, font = :bold, color = TEXT,
          halign = :left, tellwidth = false)
    Label(fig[1, 3], "Dagger.jl";
          fontsize = 22, font = :bold, color = DAGGER,
          halign = :right, tellwidth = false)
    Label(fig[2, 1:3], subtitle;
          fontsize = 18, color = accent,
          halign = :left, tellwidth = false)

    main = fig[3, 1:3] = GridLayout()
    rowsize!(root, 3, Relative(0.82))

    Box(fig[4, 1:3]; color = RULE, strokewidth = 0)
    rowsize!(root, 4, Fixed(1))

    Label(fig[5, 1], footer_left; fontsize = 16, color = MUTED, halign = :left, tellwidth = false)
    Label(fig[5, 2], footer_mid; fontsize = 16, color = MUTED, halign = :center, tellwidth = false)
    Label(fig[5, 3], footer_right; fontsize = 16, color = MUTED, halign = :right, tellwidth = false)

    return fig, main
end

function hide_ticks!(ax::Axis; keep_title::Bool = false)
    # Axis titles are not a decoration in this Makie version.
    hidedecorations!(ax)
    hidespines!(ax)
    ax.backgroundcolor = PANEL
    return ax
end

function fit_square!(ax::Axis, xs, ys; pad = 0.03)
    xmin, xmax = extrema(xs)
    ymin, ymax = extrema(ys)
    xc = (xmin + xmax) / 2
    yc = (ymin + ymax) / 2
    r = max(xmax - xmin, ymax - ymin) / 2 + pad
    xlims!(ax, xc - r, xc + r)
    ylims!(ax, yc - r, yc + r)
    return ax
end

function fmt_rel(rel::Real)
    # ASCII on purpose: the pitch font lacks U+2016 double vertical line.
    return "||r|| / ||b|| = " * string(round(Float64(rel); sigdigits = 3))
end

function fmt_blocks(part)
    bs = part.blocksize
    return "Blocks(" * join(Int.(bs), ", ") * ")"
end

function fmt_time(t::Real)
    return "t = " * string(round(t; digits = 3))
end

smoothstep(t) = let x = clamp(t, 0, 1); x * x * (3 - 2x); end
smootherstep(t) = let x = clamp(t, 0, 1); x * x * x * (x * (6x - 15) + 10); end

# 0→1→0 over [0,1], smooth, loop-friendly.
function pulse01(θ)
    s = sin(π * clamp(θ, 0, 1))^2
    return s
end

function lerp(a::Real, b::Real, t::Real)
    return (1 - t) * a + t * b
end
function lerp(a::AbstractArray, b::AbstractArray, t::Real)
    return (1 - t) .* a .+ t .* b
end

"""
Cross-fade the last `nblend` frames into the first ones so an mp4 loops cleanly.
`frames` is a Vector of Arrays.
"""
function loop_blend!(frames::Vector, nblend::Int = 18)
    n = length(frames)
    nblend = min(nblend, n ÷ 3)
    nblend < 2 && return frames
    for i in 1:nblend
        α = i / (nblend + 1)
        frames[end - nblend + i] = lerp(frames[end - nblend + i], frames[i], α)
    end
    return frames
end

function ensure_dir(path)
    isdir(path) || mkpath(path)
    return path
end

function frame_dir(name::AbstractString)
    d = joinpath(FRAME_DIR, name)
    isdir(d) && rm(d; recursive = true)
    return ensure_dir(d)
end

function save_frame(fig::Figure, dir::AbstractString, i::Integer)
    path = joinpath(dir, "frame_$(lpad(i, 4, '0')).png")
    save(path, fig; px_per_unit = 1)
    return path
end

function encode_mp4(name::AbstractString; fps::Integer = FPS, crf::Integer = 21)
    frames = joinpath(FRAME_DIR, name)
    ensure_dir(VIDEO_DIR)
    out = joinpath(VIDEO_DIR, name * ".mp4")
    pattern = joinpath(frames, "frame_%04d.png")
    cmd = `ffmpeg -y -loglevel error -framerate $fps -i $pattern
           -c:v libx264 -pix_fmt yuv420p -preset medium -crf $crf
           -movflags +faststart $out`
    run(cmd)
    # Drop PNG intermediates once the mp4 is on disk.
    rm(frames; recursive = true, force = true)
    bytes = filesize(out)
    @info "encoded" name out mb = round(bytes / 1024^2; digits = 2)
    return out
end

function git_sha()
    try
        return readchomp(Cmd(`git rev-parse --short=12 HEAD`;
                             dir = joinpath(PITCH_ROOT, "..")))
    catch
        return "unknown"
    end
end
