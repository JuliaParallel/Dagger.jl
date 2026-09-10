# Q1 plane-strain cantilever. Smoothed aggregation with rigid-body near-nullspace.

function sim_elasticity(; nelx=16, nely=6, ntiles=4, solve_only=false)
    mesh = q1_elasticity_cantilever(nelx, nely; λ=1.2, μ=1.0)
    n = size(mesh.A, 1)
    Apart, bpart = square_blocks(n; ntiles)
    Npart = Blocks(Apart.blocksize[1], size(mesh.N, 2))
    DA = distribute(mesh.A, Apart)
    Db = distribute(mesh.b, bpart)
    DN = distribute(mesh.N, Npart)
    M = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=DN,
                                                 max_levels=4, max_coarse=24,
                                                 presweeps=2, postsweeps=2)
    tsolve = @elapsed begin
        x, stats, rel = solve_gmres(DA, Db, M; itmax=250, rtol=1e-10, memory=80)
    end
    ufree = collect(x)
    u = expand_free(ufree, mesh.free, mesh.ndof)
    ux = reshape(u[1:2:end], mesh.ngx, mesh.ngy)
    uy = reshape(u[2:2:end], mesh.ngx, mesh.ngy)
    umag = hypot.(ux, uy)
    meta = (;
        name = "02_elasticity",
        title = "Linear elasticity",
        subtitle = "solved with Dagger SmoothedAggregationPreconditioner  |  nullspace = rigid-body modes",
        rel,
        niter = stats.niter,
        blocks = Apart,
        n,
        nelx, nely,
        nmodes = 3,
        tsolve,
        api = "SmoothedAggregationPreconditioner(A; nullspace=N) + Krylov.gmres",
    )
    @info "elasticity" rel niter=stats.niter tsolve nmodes=M.nmodes
    solve_only && return meta
    render_elasticity(mesh, ux, uy, umag, meta)
    return meta
end

function render_elasticity(mesh, ux, uy, umag, meta)
    apply_pitch_theme!()
    dir = frame_dir(meta.name)
    ngx, ngy = mesh.ngx, mesh.ngy
    X = reshape(mesh.xs, ngx, ngy)
    Y = reshape(mesh.ys, ngx, ngy)
    # Scale the warp so the tip drop is ~0.28 of the beam height.
    tip = maximum(abs, uy)
    scale = tip > 0 ? (0.28 * mesh.hy * (ngy - 1)) / tip : 1.0
    vmax = maximum(umag)
    # Quad faces (two triangles each) in node-major order.
    pts0 = [Point2f(X[i, j], Y[i, j]) for j in 1:ngy for i in 1:ngx]
    nf = 2 * mesh.nelx * mesh.nely
    faces = Matrix{Int}(undef, nf, 3)
    k = 0
    for ey in 1:mesh.nely, ex in 1:mesh.nelx
        n1 = ex + (ey - 1) * ngx
        n2 = n1 + 1
        n3 = n2 + ngx
        n4 = n1 + ngx
        k += 1; faces[k, 1] = n1; faces[k, 2] = n2; faces[k, 3] = n3
        k += 1; faces[k, 1] = n1; faces[k, 2] = n3; faces[k, 3] = n4
    end
    colors = vec(umag)
    # Load-unload pulse for a loopable clip.
    for f in 1:NFRAMES
        θ = (f - 1) / NFRAMES
        α = pulse01(θ)
        αe = smootherstep(α)
        pts = [Point2f(X[i, j] + αe * scale * ux[i, j],
                       Y[i, j] + αe * scale * uy[i, j])
               for j in 1:ngy for i in 1:ngx]
        fig, main = pitch_chrome(ACCENT.elasticity;
            title = meta.title,
            subtitle = meta.subtitle,
            footer_left = fmt_rel(meta.rel),
            footer_mid = fmt_blocks(meta.blocks) * "  |  Q1 $(meta.nelx)x$(meta.nely)",
            footer_right = "nullspace = 3 rigid modes  |  load $(round(αe; digits=2))")
        ax = Axis(main[1, 1]; aspect = DataAspect())
        hide_ticks!(ax)
        # Ghost undeformed outline
        lines!(ax, X[:, 1], Y[:, 1]; color = (MUTED, 0.35), linewidth = 1.0)
        lines!(ax, X[:, end], Y[:, end]; color = (MUTED, 0.35), linewidth = 1.0)
        lines!(ax, X[1, :], Y[1, :]; color = (MUTED, 0.35), linewidth = 1.0)
        lines!(ax, X[end, :], Y[end, :]; color = (MUTED, 0.35), linewidth = 1.0)
        mesh!(ax, pts, faces; color = colors, colormap = CMAP.elasticity,
              colorrange = (0, max(vmax, eps())))
        wireframe = true
        if wireframe
            # Light warped grid
            for j in 1:ngy
                lines!(ax,
                       [p[1] for p in pts[(j-1)*ngx+1:j*ngx]],
                       [p[2] for p in pts[(j-1)*ngx+1:j*ngx]];
                       color = (TEXT, 0.12), linewidth = 0.6)
            end
            for i in 1:ngx
                lines!(ax,
                       [pts[i + (j-1)*ngx][1] for j in 1:ngy],
                       [pts[i + (j-1)*ngx][2] for j in 1:ngy];
                       color = (TEXT, 0.12), linewidth = 0.6)
            end
        end
        Colorbar(main[1, 2], colormap = CMAP.elasticity,
                 colorrange = (0, max(vmax, eps())),
                 label = "|u|", width = 16, labelsize = 14)
        save_frame(fig, dir, f)
        (f % 40 == 0) && @info "elasticity frame" f
    end
    encode_mp4(meta.name)
    return nothing
end
