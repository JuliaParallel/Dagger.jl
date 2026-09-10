# Mixed precision: FP32 block-Jacobi preconditioner, FP64 Krylov (CG).

function sim_mixedprec(; nx=48, ny=48, ntiles=4, solve_only=false)
    n = nx * ny
    Apart, bpart = square_blocks(n; ntiles)
    DA64 = assemble_poisson2d(nx, ny, Apart, Float64)
    DA32 = assemble_poisson2d(nx, ny, Apart, Float32)
    xs, ys = grid_xy(nx, ny)
    X = repeat(xs, 1, ny)
    Y = repeat(ys', nx, 1)
    b = vec(@. exp(-((X - 0.5)^2 + (Y - 0.5)^2) / 0.04) +
               0.35 * exp(-((X - 0.22)^2 + (Y - 0.72)^2) / 0.012))
    Db = distribute(b, bpart)
    P = Dagger.BlockJacobiPreconditioner(DA32)
    tsolve = @elapsed begin
        x, stats, rel = solve_cg(DA64, Db, P; itmax=250, rtol=1e-10, history=true)
    end
    u = reshape(collect(x), nx, ny)
    hist = residual_history(stats)
    meta = (;
        name = "06_mixedprec",
        title = "Mixed-precision Poisson",
        subtitle = "solved with Dagger BlockJacobiPreconditioner  |  FP32 M, FP64 Krylov.cg",
        rel,
        niter = stats.niter,
        blocks = Apart,
        grid = (nx, ny),
        n,
        tsolve,
        hist,
        api = "BlockJacobiPreconditioner(::DMatrix{Float32}) + Krylov.cg(::DMatrix{Float64})",
    )
    @info "mixedprec" rel niter=stats.niter tsolve
    solve_only && return meta
    render_mixedprec(u, xs, ys, meta)
    return meta
end

function render_mixedprec(u, xs, ys, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    vmax = maximum(abs, u)
    hist = meta.hist
    # Build a residual sparkline even if Krylov did not keep history.
    if isempty(hist)
        hist = [1.0, meta.rel]
    end
    hist = max.(hist, 1e-16)
    niter = max(meta.niter, 1)
    for f in 1:NFRAMES
        θ = (f - 1) / (NFRAMES - 1)
        α = smootherstep(θ)
        fig, main = pitch_chrome(ACCENT.mixed;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  $(meta.grid[1])x$(meta.grid[2])",
            footer_right = "FP32 PC  |  FP64 Krylov  |  $(meta.niter) CG iters")
        ax = Axis(main[1, 1]; aspect = DataAspect())
        hide_ticks!(ax)
        heatmap!(ax, xs, ys, α .* u;
                 colormap = CMAP.mixed, colorrange = (0, vmax),
                 interpolate = true)
        fit_square!(ax, xs, ys)
        Colorbar(main[1, 2], colormap = CMAP.mixed, colorrange = (0, vmax),
                 label = "u", width = 14)
        # Residual sparkline inset
        inset = Axis(main[1, 1],
                     width = Relative(0.28), height = Relative(0.22),
                     halign = 0.08, valign = 0.12,
                     backgroundcolor = (BG, 0.78),
                     title = "residual history", titlesize = 13, titlecolor = MUTED)
        kshow = max(2, round(Int, 1 + α * (length(hist) - 1)))
        xs_h = 1:kshow
        lines!(inset, xs_h, hist[1:kshow]; color = ACCENT.mixed, linewidth = 2.0)
        inset.yscale = log10
        ylims!(inset, (min(minimum(hist), 1e-12) * 0.5, maximum(hist) * 1.2))
        xlims!(inset, (1, length(hist)))
        hidespines!(inset)
        inset.xticklabelcolor = MUTED
        inset.yticklabelcolor = MUTED
        inset.xticklabelsize = 11
        inset.yticklabelsize = 11
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "mixedprec frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
