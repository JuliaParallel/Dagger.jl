# Transient 2-D heat, implicit backward-Euler, GeometricMultigrid V-cycle.
# Hierarchy is built once and reused every step (constant κ).

function sim_heat(; nx=40, ny=40, ntiles=4, nsteps=36, dt=0.012, κ=0.18,
                  solve_only=false)
    n = nx * ny
    Apart, bpart = square_blocks(n; ntiles)
    DA = assemble_heat_be(nx, ny, Apart; dt, κ)
    M = Dagger.GeometricMultigrid(DA; grid=(nx, ny), max_levels=4, max_coarse=32,
                                  presweeps=2, postsweeps=2)
    xs, ys = grid_xy(nx, ny)
    X = repeat(xs, 1, ny)
    Y = repeat(ys', nx, 1)

    function source_field(t)
        ω = 2π / (nsteps * dt)
        cx = 0.5 + 0.22 * cos(ω * t)
        cy = 0.5 + 0.22 * sin(ω * t)
        return @. exp(-((X - cx)^2 + (Y - cy)^2) / 0.010)
    end

    u = zeros(n)
    snaps = Vector{Matrix{Float64}}(undef, nsteps)
    rels = Float64[]
    niters = Int[]
    tsolve = 0.0
    for s in 1:nsteps
        t = s * dt
        rhs = u .+ dt .* vec(source_field(t))
        Db = distribute(rhs, bpart)
        Du = distribute(u, bpart)
        wall = @elapsed begin
            x, stats, rel = solve_cg(DA, Db, M; itmax=80, rtol=1e-10)
            u = collect(x)
        end
        tsolve += wall
        push!(rels, rel)
        push!(niters, stats.niter)
        snaps[s] = reshape(u, nx, ny)
        @info "heat step" s rel niter=stats.niter wall
    end
    rel = maximum(rels)  # worst step; last is what we print
    rel_last = rels[end]
    meta = (;
        name = "01_heat",
        title = "Transient heat",
        subtitle = "solved with Dagger GeometricMultigrid  |  implicit backward-Euler",
        rel = rel_last,
        rels,
        niter = niters[end],
        niter_mean = mean(niters),
        blocks = Apart,
        grid = (nx, ny),
        n,
        dt, κ,
        tsolve,
        api = "GeometricMultigrid + Krylov.cg, hierarchy reused each step",
    )
    solve_only && return meta
    render_heat(snaps, xs, ys, meta)
    return meta
end

function render_heat(snaps, xs, ys, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    nsnaps = length(snaps)
    # Interpolate snapshots onto a 15 s / 24 fps timeline, then loop-blend.
    dense = Vector{Matrix{Float64}}(undef, NFRAMES)
    for f in 1:NFRAMES
        θ = (f - 1) / NFRAMES * nsnaps
        i0 = clamp(floor(Int, θ) + 1, 1, nsnaps)
        i1 = min(i0 + 1, nsnaps)
        t = θ - (i0 - 1)
        dense[f] = lerp(snaps[i0], snaps[i1], t)
    end
    loop_blend!(dense, 20)
    vmax = quantile(filter(isfinite, reduce(vcat, vec.(snaps))), 0.995)
    vmax = max(vmax, 1e-8)
    for f in 1:NFRAMES
        tphys = (f - 1) / NFRAMES * nsnaps * meta.dt
        fig, main = pitch_chrome(ACCENT.heat;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  $(meta.grid[1])x$(meta.grid[2])",
            footer_right = fmt_time(tphys) * "   kappa = $(meta.κ)")
        ax = Axis(main[1, 1]; aspect = DataAspect())
        hide_ticks!(ax)
        heatmap!(ax, xs, ys, dense[f];
                 colormap = CMAP.heat, colorrange = (0, vmax),
                 interpolate = true)
        fit_square!(ax, xs, ys)
        Colorbar(main[1, 2], colormap = CMAP.heat, colorrange = (0, vmax),
                 label = "T", width = 14)
        colgap!(main, 8)
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "heat frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
