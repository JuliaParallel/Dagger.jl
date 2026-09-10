# Nonsymmetric convection–diffusion plume. GMRES + restricted additive Schwarz.
# AIR is not on this branch; RAS + HMIS-ready GMRES is the honest pitch.

function sim_convection(; nx=36, ny=36, ntiles=4, nsteps=28, dt=0.035, ε=0.045,
                        solve_only=false)
    n = nx * ny
    Apart, bpart = square_blocks(n; ntiles)
    # Recirculating gyre — the plume wraps, which loops more honestly than a jet.
    vx(x, y) =  sin(π * x)^2 * sin(2π * y)
    vy(x, y) = -sin(π * y)^2 * sin(2π * x)
    DA = assemble_convdiff_be(nx, ny, Apart; ε, vx, vy, dt)
    M = Dagger.AdditiveSchwarzPreconditioner(DA; overlap=1, type=:restrict)
    xs, ys = grid_xy(nx, ny)
    X = repeat(xs, 1, ny)
    Y = repeat(ys', nx, 1)
    src0 = @. exp(-((X - 0.35)^2 + (Y - 0.55)^2) / 0.008)

    u = zeros(n)
    snaps = Vector{Matrix{Float64}}(undef, nsteps)
    rels = Float64[]
    niters = Int[]
    tsolve = 0.0
    for s in 1:nsteps
        amp = sin(π * s / nsteps)^2
        rhs = u .+ dt .* amp .* vec(src0)
        Db = distribute(rhs, bpart)
        wall = @elapsed begin
            x, stats, rel = solve_gmres(DA, Db, M; itmax=120, rtol=1e-8, memory=50)
            u = collect(x)
        end
        tsolve += wall
        push!(rels, rel)
        push!(niters, stats.niter)
        snaps[s] = reshape(u, nx, ny)
        @info "convection step" s rel niter=stats.niter wall
    end
    # Quiver samples
    sx = xs[2:3:end]
    sy = ys[2:3:end]
    UX = [vx(x, y) for x in sx, y in sy]
    UY = [vy(x, y) for x in sx, y in sy]
    meta = (;
        name = "03_convection",
        title = "Convection–diffusion",
        subtitle = "solved with Dagger AdditiveSchwarzPreconditioner  |  GMRES  |  RAS overlap 1",
        rel = rels[end],
        rels,
        niter = niters[end],
        niter_mean = mean(niters),
        blocks = Apart,
        grid = (nx, ny),
        n, dt, ε,
        tsolve,
        api = "AdditiveSchwarzPreconditioner(overlap=1, type=:restrict) + Krylov.gmres",
    )
    solve_only && return meta
    render_convection(snaps, xs, ys, sx, sy, UX, UY, meta)
    return meta
end

function render_convection(snaps, xs, ys, sx, sy, UX, UY, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    nsnaps = length(snaps)
    dense = Vector{Matrix{Float64}}(undef, NFRAMES)
    for f in 1:NFRAMES
        θ = (f - 1) / NFRAMES * nsnaps
        i0 = clamp(floor(Int, θ) + 1, 1, nsnaps)
        i1 = min(i0 + 1, nsnaps)
        dense[f] = lerp(snaps[i0], snaps[i1], θ - (i0 - 1))
    end
    loop_blend!(dense, 22)
    vmax = quantile(filter(isfinite, reduce(vcat, vec.(snaps))), 0.99)
    vmax = max(vmax, 1e-8)
    for f in 1:NFRAMES
        tphys = (f - 1) / NFRAMES * nsnaps * meta.dt
        fig, main = pitch_chrome(ACCENT.convection;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  $(meta.grid[1])x$(meta.grid[2])",
            footer_right = fmt_time(tphys) * "   eps = $(meta.ε)")
        ax = Axis(main[1, 1]; aspect = DataAspect())
        hide_ticks!(ax)
        heatmap!(ax, xs, ys, dense[f];
                 colormap = CMAP.convection, colorrange = (0, vmax),
                 interpolate = true)
        arrows2d!(ax, sx, sy, UX, UY;
                  lengthscale = 0.045,
                  shaftwidth = 1.4,
                  tipwidth = 6,
                  tiplength = 7,
                  color = (TEXT, 0.45))
        fit_square!(ax, xs, ys)
        Colorbar(main[1, 2], colormap = CMAP.convection, colorrange = (0, vmax),
                 label = "c", width = 14)
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "convection frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
