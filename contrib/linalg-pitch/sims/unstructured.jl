# Circular-domain Poisson with scrambled numbering, METIS tiles, GlobalAMG.

function sim_unstructured(; nx=36, ny=36, ntiles=4, solve_only=false)
    disk = disk_poisson(nx, ny; radius=0.48)
    n = disk.n
    Apart, bpart = square_blocks(n; ntiles)
    nparts = cld(n, Apart.blocksize[1])
    # Geometric numbering first (for a fair METIS graph), then scramble like a
    # bad mesh file, then let Metis recover locality.
    rng = MersenneTwister(2026)
    scramble = randperm(rng, n)
    Ascr = disk.A[scramble, scramble]
    # Source near the centre, in geometric unknowns, then scramble.
    bgeom = zeros(n)
    for j in 1:ny, i in 1:nx
        k = disk.idx[i, j]
        k == 0 && continue
        bgeom[k] = exp(-(disk.xs[i]^2 + disk.ys[j]^2) / 0.04)
    end
    bscr = bgeom[scramble]

    parts_scr = Dagger.partition_graph(Metis, Ascr, nparts)
    perm = Dagger.partition_perm(parts_scr)
    DA = distribute(Ascr, Apart; perm)
    Db = distribute(bscr, bpart; perm)
    M = Dagger.SmoothedAggregationPreconditioner(DA; max_levels=4, max_coarse=32,
                                                 coarsen=:hmis,
                                                 presweeps=2, postsweeps=2)
    tsolve = @elapsed begin
        x, stats, rel = solve_cg(DA, Db, M; itmax=200, rtol=1e-10)
    end
    xperm = collect(x)
    # Back to scrambled, then geometric.
    xscr = similar(xperm)
    xscr[perm] = xperm
    xgeom = similar(xscr)
    xgeom[scramble] = xscr

    # Partition ids on the geometric mesh: invert scramble, then parts_scr.
    parts_geom = similar(parts_scr)
    parts_geom[scramble] = parts_scr

    Zsol = disk_embed(xgeom, disk.idx, nx, ny)
    Zpart = disk_embed(Float64.(parts_geom), disk.idx, nx, ny)
    # Scrambled-index “noise” — hash of the scrambled id, looks unstructured.
    noise = zeros(n)
    for j in 1:ny, i in 1:nx
        k = disk.idx[i, j]
        k == 0 && continue
        noise[k] = mod(scramble[k], nparts) + 1
    end
    Znoise = disk_embed(noise, disk.idx, nx, ny)

    meta = (;
        name = "05_unstructured",
        title = "Graph-partitioned Poisson",
        subtitle = "solved with Dagger GlobalAMG  |  repartition(; partitioner = Metis)",
        rel,
        niter = stats.niter,
        blocks = Apart,
        n,
        nparts,
        grid = (nx, ny),
        tsolve,
        api = "repartition / distribute(; partitioner=Metis) + SmoothedAggregationPreconditioner",
    )
    @info "unstructured" rel niter=stats.niter tsolve nparts
    solve_only && return meta
    render_unstructured(Znoise, Zpart, Zsol, disk.xs, disk.ys, meta)
    return meta
end

function render_unstructured(Znoise, Zpart, Zsol, xs, ys, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    nparts = meta.nparts
    smin, smax = extrema(filter(isfinite, vec(Zsol)))
    # Timeline: 0–0.28 scramble, 0.28–0.50 Metis parts, 0.50–1.00 solution.
    for f in 1:NFRAMES
        θ = (f - 1) / (NFRAMES - 1)
        beat = if θ < 0.28
            "scrambled numbering"
        elseif θ < 0.50
            "METIS parts"
        else
            "GlobalAMG solution"
        end
        fig, main = pitch_chrome(ACCENT.unstructured;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  $(meta.nparts) METIS parts",
            footer_right = beat * "  |  n = $(meta.n) on a disk")
        ax = Axis(main[1, 1]; aspect = DataAspect())
        hide_ticks!(ax)
        if θ < 0.28
            heatmap!(ax, collect(xs), collect(ys), Znoise;
                     colormap = PART_COLORS[1:max(nparts, 1)],
                     colorrange = (1, max(nparts, 2)),
                     nan_color = :transparent)
        elseif θ < 0.50
            α = smootherstep((θ - 0.28) / 0.22)
            Z = (1 - α) .* Znoise .+ α .* Zpart
            heatmap!(ax, collect(xs), collect(ys), Z;
                     colormap = PART_COLORS[1:max(nparts, 1)],
                     colorrange = (1, max(nparts, 2)),
                     nan_color = :transparent)
        else
            α = smootherstep((θ - 0.50) / 0.50)
            heatmap!(ax, collect(xs), collect(ys), Zsol;
                     colormap = CMAP.unstructured,
                     colorrange = (smin, smax),
                     nan_color = :transparent,
                     interpolate = true)
            Colorbar(main[1, 2], colormap = CMAP.unstructured,
                     colorrange = (smin, smax), label = "u", width = 14)
            if α < 0.35
                β = 1 - α / 0.35
                heatmap!(ax, collect(xs), collect(ys), Zpart;
                         colormap = PART_COLORS[1:max(nparts, 1)],
                         colorrange = (1, max(nparts, 2)),
                         nan_color = :transparent,
                         alpha = β)
            end
        end
        fit_square!(ax, xs, ys; pad = 0.02)
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "unstructured frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
