# Public-API assembly + residual helpers for the pitch simulations.
# Prefer `sparse(I, J, V, m, n, Blocks)` over a host CSC + `distribute`.

using Dagger
using SparseArrays
using LinearAlgebra
using Krylov
using AlgebraicMultigrid
using Metis
using Random
using Statistics

const Blocks = Dagger.Blocks

function true_relres(A, x, b)
    r = similar(b)
    mul!(r, A, x)
    axpy!(-one(eltype(b)), b, r)
    return Float64(norm(r) / max(norm(b), eps(Float64)))
end

function square_blocks(n::Integer; ntiles::Integer = 4)
    k = cld(n, ntiles)
    return Blocks(k, k), Blocks(k)
end

"""Column-major 2-D 5-point Laplacian COO (Dirichlet via missing neighbours)."""
function poisson2d_coo(nx::Integer, ny::Integer, ::Type{T} = Float64; scale = one(T)) where T
    n = nx * ny
    I = Int[]; J = Int[]; V = T[]
    sizehint!(I, 5n); sizehint!(J, 5n); sizehint!(V, 5n)
    idx(i, j) = i + (j - 1) * nx
    @inbounds for j in 1:ny, i in 1:nx
        k = idx(i, j)
        push!(I, k); push!(J, k); push!(V, T(4) * scale)
        if i > 1;  push!(I, k); push!(J, idx(i - 1, j)); push!(V, -scale); end
        if i < nx; push!(I, k); push!(J, idx(i + 1, j)); push!(V, -scale); end
        if j > 1;  push!(I, k); push!(J, idx(i, j - 1)); push!(V, -scale); end
        if j < ny; push!(I, k); push!(J, idx(i, j + 1)); push!(V, -scale); end
    end
    return I, J, V, n
end

function assemble_poisson2d(nx, ny, part::Blocks{2}, ::Type{T} = Float64; scale = one(T)) where T
    I, J, V, n = poisson2d_coo(nx, ny, T; scale)
    return sparse(I, J, V, n, n, part)
end

function identity_plus!(I, J, V, n, coeff)
    @inbounds for k in 1:n
        push!(I, k); push!(J, k); push!(V, coeff)
    end
    return I, J, V
end

"""Implicit backward-Euler heat operator `I + dt * κ / h² * L` as a tiled `DMatrix`."""
function assemble_heat_be(nx, ny, part::Blocks{2}, ::Type{T} = Float64; dt, κ) where T
    h = 1 / (nx + 1)
    scale = T(dt * κ / h^2)
    I, J, V, n = poisson2d_coo(nx, ny, T; scale)
    identity_plus!(I, J, V, n, one(T))
    return sparse(I, J, V, n, n, part)
end

"""Upwind convection–diffusion `ε L / h² + C(v)` (steady operator)."""
function assemble_convdiff(nx, ny, part::Blocks{2}, ::Type{T} = Float64; ε, vx, vy) where T
    n = nx * ny
    h = 1 / (nx + 1)
    I = Int[]; J = Int[]; V = T[]
    idx(i, j) = i + (j - 1) * nx
    # diffusion
    Id, Jd, Vd, _ = poisson2d_coo(nx, ny, T; scale = T(ε / h^2))
    append!(I, Id); append!(J, Jd); append!(V, Vd)
    # upwind convection
    @inbounds for j in 1:ny, i in 1:nx
        k = idx(i, j)
        x = i * h; y = j * h
        vxi = T(vx(x, y)); vyi = T(vy(x, y))
        # x
        if vxi ≥ 0
            push!(I, k); push!(J, k); push!(V, vxi / T(h))
            if i > 1
                push!(I, k); push!(J, idx(i - 1, j)); push!(V, -vxi / T(h))
            end
        else
            push!(I, k); push!(J, k); push!(V, -vxi / T(h))
            if i < nx
                push!(I, k); push!(J, idx(i + 1, j)); push!(V, vxi / T(h))
            end
        end
        # y
        if vyi ≥ 0
            push!(I, k); push!(J, k); push!(V, vyi / T(h))
            if j > 1
                push!(I, k); push!(J, idx(i, j - 1)); push!(V, -vyi / T(h))
            end
        else
            push!(I, k); push!(J, k); push!(V, -vyi / T(h))
            if j < ny
                push!(I, k); push!(J, idx(i, j + 1)); push!(V, vyi / T(h))
            end
        end
    end
    return sparse(I, J, V, n, n, part)
end

"""Implicit BE convection–diffusion: `I + dt * (εL/h² + C)`."""
function assemble_convdiff_be(nx, ny, part::Blocks{2}, ::Type{T} = Float64; ε, vx, vy, dt) where T
    n = nx * ny
    h = 1 / (nx + 1)
    I = Int[]; J = Int[]; V = T[]
    idx(i, j) = i + (j - 1) * nx
    Id, Jd, Vd, _ = poisson2d_coo(nx, ny, T; scale = T(dt * ε / h^2))
    append!(I, Id); append!(J, Jd); append!(V, Vd)
    @inbounds for j in 1:ny, i in 1:nx
        k = idx(i, j)
        x = i * h; y = j * h
        vxi = T(dt) * T(vx(x, y)); vyi = T(dt) * T(vy(x, y))
        if vxi ≥ 0
            push!(I, k); push!(J, k); push!(V, vxi / T(h))
            i > 1 && (push!(I, k); push!(J, idx(i - 1, j)); push!(V, -vxi / T(h)))
        else
            push!(I, k); push!(J, k); push!(V, -vxi / T(h))
            i < nx && (push!(I, k); push!(J, idx(i + 1, j)); push!(V, vxi / T(h)))
        end
        if vyi ≥ 0
            push!(I, k); push!(J, k); push!(V, vyi / T(h))
            j > 1 && (push!(I, k); push!(J, idx(i, j - 1)); push!(V, -vyi / T(h)))
        else
            push!(I, k); push!(J, k); push!(V, -vyi / T(h))
            j < ny && (push!(I, k); push!(J, idx(i, j + 1)); push!(V, vyi / T(h)))
        end
    end
    identity_plus!(I, J, V, n, one(T))
    return sparse(I, J, V, n, n, part)
end

function grid_xy(nx, ny)
    h = 1 / (nx + 1)
    xs = [(i * h) for i in 1:nx]
    ys = [(j * h) for j in 1:ny]
    return xs, ys
end

function vec_to_grid(v::AbstractVector, nx, ny)
    return reshape(collect(v), nx, ny)
end

# ---------------------------------------------------------------------------
# Q1 plane-strain elasticity (same element as test/array/linalg/nearnullspace.jl)
# ---------------------------------------------------------------------------

function q1_stiffness(hx, hy, λ, μ)
    D = [λ + 2μ  λ      0.0
         λ       λ + 2μ 0.0
         0.0     0.0    μ]
    g = 1 / sqrt(3)
    Ke = zeros(8, 8)
    detJ = hx * hy / 4
    for ξ in (-g, g), η in (-g, g)
        dNdξ = [-(1 - η) / 4, (1 - η) / 4, (1 + η) / 4, -(1 + η) / 4]
        dNdη = [-(1 - ξ) / 4, -(1 + ξ) / 4, (1 + ξ) / 4, (1 - ξ) / 4]
        dNdx = (2 / hx) .* dNdξ
        dNdy = (2 / hy) .* dNdη
        Bm = zeros(3, 8)
        for a in 1:4
            Bm[1, 2a - 1] = dNdx[a]
            Bm[2, 2a]     = dNdy[a]
            Bm[3, 2a - 1] = dNdy[a]
            Bm[3, 2a]     = dNdx[a]
        end
        Ke .+= (Bm' * D * Bm) .* detJ
    end
    return Ke
end

function q1_elasticity_cantilever(nelx, nely; λ=1.0, μ=1.0,
                                  Lx=Float64(nelx), Ly=Float64(nely))
    hx, hy = Lx / nelx, Ly / nely
    ngx, ngy = nelx + 1, nely + 1
    nnode = ngx * ngy
    ndof = 2 * nnode
    I, J, V = Int[], Int[], Float64[]
    Ke = q1_stiffness(hx, hy, λ, μ)
    for ey in 1:nely, ex in 1:nelx
        n1 = ex + (ey - 1) * ngx
        n2 = n1 + 1
        n3 = n2 + ngx
        n4 = n1 + ngx
        nodes = (n1, n2, n3, n4)
        edof = Vector{Int}(undef, 8)
        for a in 1:4
            edof[2a - 1] = 2 * nodes[a] - 1
            edof[2a]     = 2 * nodes[a]
        end
        for a in 1:8, b in 1:8
            push!(I, edof[a]); push!(J, edof[b]); push!(V, Ke[a, b])
        end
    end
    Afull = sparse(I, J, V, ndof, ndof)
    Bfull = zeros(ndof, 3)
    bfull = zeros(ndof)
    fixed = Int[]
    xs = zeros(nnode); ys = zeros(nnode)
    for j in 1:ngy, i in 1:ngx
        n = i + (j - 1) * ngx
        x = (i - 1) * hx
        y = (j - 1) * hy
        xs[n] = x; ys[n] = y
        Bfull[2n - 1, 1] = 1
        Bfull[2n,     2] = 1
        Bfull[2n - 1, 3] = -y
        Bfull[2n,     3] = x
        if i == 1
            push!(fixed, 2n - 1, 2n)
        end
        if i == ngx
            bfull[2n] -= 1 / ngy
        end
    end
    free = setdiff(1:ndof, unique(fixed))
    return (; A = Afull[free, free], b = bfull[free], N = Bfull[free, :],
              ngx, ngy, hx, hy, xs, ys, free, ndof, nnode, nelx, nely)
end

function expand_free(xfree::AbstractVector, free, ndof)
    u = zeros(ndof)
    u[free] = collect(xfree)
    return u
end

# ---------------------------------------------------------------------------
# Circular-domain 5-point Poisson (unstructured numbering story)
# ---------------------------------------------------------------------------

function disk_poisson(nx, ny; radius = 0.48)
    xs = range(-0.5, 0.5; length = nx)
    ys = range(-0.5, 0.5; length = ny)
    mask = falses(nx, ny)
    for j in 1:ny, i in 1:nx
        mask[i, j] = xs[i]^2 + ys[j]^2 <= radius^2
    end
    idx = zeros(Int, nx, ny)
    n = 0
    for j in 1:ny, i in 1:nx
        if mask[i, j]
            n += 1
            idx[i, j] = n
        end
    end
    I = Int[]; J = Int[]; V = Float64[]
    hx = xs[2] - xs[1]; hy = ys[2] - ys[1]
    sx, sy = 1 / hx^2, 1 / hy^2
    for j in 1:ny, i in 1:nx
        k = idx[i, j]
        k == 0 && continue
        diag = 0.0
        for (di, dj, s) in ((-1, 0, sx), (1, 0, sx), (0, -1, sy), (0, 1, sy))
            ii, jj = i + di, j + dj
            if 1 <= ii <= nx && 1 <= jj <= ny && idx[ii, jj] != 0
                push!(I, k); push!(J, idx[ii, jj]); push!(V, -s)
                diag += s
            else
                # homogeneous Dirichlet ghost
                diag += s
            end
        end
        push!(I, k); push!(J, k); push!(V, diag)
    end
    A = sparse(I, J, V, n, n)
    return (; A, mask, idx, n, xs, ys, nx, ny)
end

function disk_embed(v::AbstractVector, idx, nx, ny)
    Z = fill(NaN, nx, ny)
    @inbounds for j in 1:ny, i in 1:nx
        k = idx[i, j]
        k != 0 && (Z[i, j] = v[k])
    end
    return Z
end

# ---------------------------------------------------------------------------
# Krylov wrappers — always return the un-preconditioned residual (lesson 19/32)
# ---------------------------------------------------------------------------

function solve_cg(A, b, M; atol=1e-12, rtol=1e-10, itmax=400, history=false)
    x, stats = Krylov.cg(A, b; M, atol, rtol, itmax, history)
    return x, stats, true_relres(A, x, b)
end

function solve_gmres(A, b, M; atol=1e-12, rtol=1e-10, itmax=400, memory=60, history=false)
    n = length(b)
    x, stats = Krylov.gmres(A, b; M, atol, rtol, itmax, history,
                            memory = min(memory, n))
    return x, stats, true_relres(A, x, b)
end

function residual_history(stats)
    for name in (:residuals, :rNorms, :rNorm)
        if hasproperty(stats, name)
            v = getproperty(stats, name)
            v isa AbstractVector && !isempty(v) && return Float64.(v)
        end
    end
    return Float64[]
end
