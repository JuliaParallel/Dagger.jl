# Thermal–structure nest: heat drives a membrane displacement.
# BlockOperator + field-split BlockDiagonalPC (Jacobi on each field).

function sim_multiphysics(; nx=24, ny=24, ntiles=4, nsteps=24, solve_only=false)
    n = nx * ny
    Apart, bpart = square_blocks(n; ntiles)
    # Temperature Poisson and a slightly stiffer membrane.
    LT = assemble_poisson2d(nx, ny, Apart; scale=1.0)
    Ku = assemble_poisson2d(nx, ny, Apart; scale=1.4)
    # Thermal expansion coupling: -α I  (structure sees temperature).
    α = 0.35
    Iα = distribute(spdiagm(0 => fill(-α, n)), Apart)
    Z = nothing
    Aop = Dagger.BlockOperator(LT, Z, Iα, Ku)
    PT = Dagger.JacobiPreconditioner(LT)
    PU = Dagger.JacobiPreconditioner(Ku)
    P = Dagger.BlockDiagonalPC((PT, PU); sizes=(n, n))

    xs, ys = grid_xy(nx, ny)
    X = repeat(xs, 1, ny)
    Y = repeat(ys', nx, 1)
    function heat_load(amp)
        return @. amp * exp(-((X - 0.55)^2 + (Y - 0.45)^2) / 0.018)
    end

    Tsnaps = Vector{Matrix{Float64}}(undef, nsteps)
    Usnaps = Vector{Matrix{Float64}}(undef, nsteps)
    rels = Float64[]
    niters = Int[]
    tsolve = 0.0
    # The nest is length 2n; tile the concatenated vector with the same k.
    bpart2 = Blocks(bpart.blocksize[1])
    for s in 1:nsteps
        amp = 4.0 * sin(π * s / nsteps)^2
        q = vec(heat_load(amp))
        rhs = vcat(q, zeros(n))
        Db = distribute(rhs, bpart2)
        wall = @elapsed begin
            x, stats, rel = solve_gmres(Aop, Db, P; itmax=200, rtol=1e-8, memory=60)
            xv = collect(x)
        end
        tsolve += wall
        push!(rels, rel)
        push!(niters, stats.niter)
        Tsnaps[s] = reshape(xv[1:n], nx, ny)
        Usnaps[s] = reshape(xv[n+1:end], nx, ny)
        @info "multiphysics step" s rel niter=stats.niter wall
    end
    meta = (;
        name = "04_multiphysics",
        title = "Thermal–structure nest",
        subtitle = "solved with Dagger BlockOperator  |  BlockDiagonalPC field-split",
        # Last step is a zero load (pulse returns to 0); report a loaded step.
        rel = maximum(rels[1:max(1, end - 1)]),
        rels,
        niter = niters[max(1, end - 1)],
        niter_mean = mean(filter(>(0), niters)),
        blocks = Apart,
        grid = (nx, ny),
        n,
        tsolve,
        api = "BlockOperator(LT, nothing, -αI, Ku) + BlockDiagonalPC + Krylov.gmres",
    )
    solve_only && return meta
    render_multiphysics(Tsnaps, Usnaps, xs, ys, meta)
    return meta
end

function render_multiphysics(Tsnaps, Usnaps, xs, ys, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    nsnaps = length(Tsnaps)
    Td = Vector{Matrix{Float64}}(undef, NFRAMES)
    Ud = Vector{Matrix{Float64}}(undef, NFRAMES)
    for f in 1:NFRAMES
        θ = (f - 1) / NFRAMES * nsnaps
        i0 = clamp(floor(Int, θ) + 1, 1, nsnaps)
        i1 = min(i0 + 1, nsnaps)
        t = θ - (i0 - 1)
        Td[f] = lerp(Tsnaps[i0], Tsnaps[i1], t)
        Ud[f] = lerp(Usnaps[i0], Usnaps[i1], t)
    end
    loop_blend!(Td, 20)
    loop_blend!(Ud, 20)
    Tmax = quantile(filter(isfinite, reduce(vcat, vec.(Tsnaps))), 0.99)
    Umax = maximum(abs, reduce(vcat, vec.(Usnaps)))
    Tmax = max(Tmax, 1e-8)
    Umax = max(Umax, 1e-8)
    for f in 1:NFRAMES
        fig, main = pitch_chrome(ACCENT.multiphysics;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  $(meta.grid[1])x$(meta.grid[2]) x 2 fields",
            footer_right = "field-split Jacobi  |  BlockOperator")
        axT = Axis(main[1, 1]; aspect = DataAspect(), title = "temperature T",
                   titlecolor = MUTED, titlesize = 16)
        axU = Axis(main[1, 2]; aspect = DataAspect(), title = "displacement u",
                   titlecolor = MUTED, titlesize = 16)
        hide_ticks!(axT; keep_title = true)
        hide_ticks!(axU; keep_title = true)
        heatmap!(axT, xs, ys, Td[f];
                 colormap = CMAP.multiphysics, colorrange = (0, Tmax),
                 interpolate = true)
        heatmap!(axU, xs, ys, Ud[f];
                 colormap = CMAP.elasticity, colorrange = (0, Umax),
                 interpolate = true)
        fit_square!(axT, xs, ys)
        fit_square!(axU, xs, ys)
        Colorbar(main[1, 3], colormap = CMAP.multiphysics, colorrange = (0, Tmax),
                 label = "T", width = 12)
        Colorbar(main[1, 4], colormap = CMAP.elasticity, colorrange = (0, Umax),
                 label = "u", width = 12)
        colgap!(main, 10)
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "multiphysics frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
