# Shared GPU-resident preconditioner-apply bodies.
#
# Block-PC apply used to pin to ProcessScope and gather every GPU vector
# chunk to host, which restamped the Krylov workspace as `Array` and killed
# the GPU SpMV path. These bodies assert:
#
#   1. Jacobi / dense block-Jacobi / (when available) vendor ILU apply
#      numerically.
#   2. After apply, vector chunks are still device-resident (`check_vec`).
#   3. Dense block-Jacobi factors are vendor `LU` with device `factors`
#      (`check_device_lu`), not a host gather-then-UMFPACK.
#
# Host-only factors (sparse block-Jacobi → UMFPACK, AMG) still gather a
# temporary RHS inside the GPU-scoped apply; the DArray chunk stays on-device.
#
# Entry points:
#   Distributed x CPU  -> test/array/linalg/iterativesolvers.jl (no check_*)
#   Distributed x GPU  -> test/gpu.jl
#   MPI x GPU          -> test/mpi_gpu_suite.jl

function _gpu_pc_with_scope(f, scope)
    scope === nothing && return f()
    return Dagger.with_options(f; scope)
end

_gpu_pc_unwrap(op) = op isa Dagger.PinnedTileOperator ? op.op : op

# Inspect a pinned operator without `fetch` (which would `move` a device LU
# to the caller). Only unwrap when the chunk is local.
function _gpu_pc_inner_op(P, i::Int=1)
    raw = fetch(P.ops[i]; raw=true)
    raw isa Dagger.Chunk || return _gpu_pc_unwrap(raw)
    Dagger.root_worker_id(raw) == myid() || return nothing
    return _gpu_pc_unwrap(Dagger.MemPool.poolget(raw.handle))
end

# `DArray` chunks may be DTasks or Chunks. `fetch(::Chunk)` unwraps the value,
# so pass a Chunk through and only `fetch(; raw=true)` DTasks.
_gpu_pc_chunk(c) = c isa Dagger.Chunk ? c : fetch(c; raw=true)

_gpu_pc_laplacian(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)

function _gpu_pc_ilu_unavailable(e)
    msg = sprint(showerror, e)
    return occursin("IncompleteLU", msg) || occursin("ILU0", msg) ||
           occursin("_ilu_tile", msg)
end

"""
    test_gpu_pc_apply(; scope, check_vec, check_device_lu, T)

Jacobi + dense/sparse block-Jacobi + optional BlockILU. `check_vec(chunk)`
asserts a vector tile stayed on-device after apply. `check_device_lu(F)`
asserts a dense block-Jacobi factor is a device `LU`.
"""
function test_gpu_pc_apply(; scope=nothing, check_vec=nothing,
                           check_device_lu=nothing, T=Float32)
    n, k = 32, 8
    A_part, b_part = Blocks(k, k), Blocks(k)
    cmp_rtol = T <: AbstractFloat && sizeof(T) == 4 ? 1e-3 : 1e-8

    Random.seed!(1234)
    Asp = _gpu_pc_laplacian(T, n)
    Adense = Matrix(Asp)
    b = rand(T, n)
    yref = similar(b)
    for s in 1:k:n
        r = s:min(s + k - 1, n)
        yref[r] = Adense[r, r] \ b[r]
    end

    _gpu_pc_with_scope(scope) do
        @testset "Jacobi (dense)" begin
            DA = distribute(Adense, A_part)
            Db = distribute(b, b_part)
            P = Dagger.JacobiPreconditioner(DA)
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ (T(1) / T(4)) .* b
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end

        @testset "Jacobi (sparse)" begin
            DA = distribute(Asp, A_part)
            Db = distribute(b, b_part)
            P = Dagger.JacobiPreconditioner(DA)
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ (T(1) / T(4)) .* b
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end

        @testset "block-Jacobi (dense / vendor getrf)" begin
            DA = distribute(Adense, A_part)
            Db = distribute(b, b_part)
            P = Dagger.BlockJacobiPreconditioner(DA)
            inner = _gpu_pc_inner_op(P)
            if check_device_lu !== nothing && inner !== nothing
                @test check_device_lu(inner)
            end
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref rtol=cmp_rtol
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end

        @testset "block-Jacobi (sparse / host-factor fallback)" begin
            # Sparse BlockJacobi still builds a host LU (UMFPACK). Apply
            # gathers a temporary, but the output chunk must stay on-device.
            DA = distribute(Asp, A_part)
            Db = distribute(b, b_part)
            P = Dagger.BlockJacobiPreconditioner(DA)
            inner = _gpu_pc_inner_op(P)
            if inner !== nothing
                @test inner isa LinearAlgebra.Factorization
                if check_device_lu !== nothing
                    @test !check_device_lu(inner)
                end
            end
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref rtol=cmp_rtol
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end

        @testset "block-ILU" begin
            DA = distribute(Asp, A_part)
            Db = distribute(b, b_part)
            # Build is spawned; host-only tiles throw inside the worker task
            # when IncompleteLU and vendor ILU0 are both absent.
            P = Dagger.BlockILUPreconditioner(DA)
            built = try
                foreach(wait, P.ops)
                true
            catch e
                _gpu_pc_ilu_unavailable(e) || rethrow()
                false
            end
            if !built
                @test true  # host ILU unavailable and no vendor ILU0
            else
                inner = _gpu_pc_inner_op(P)
                y = similar(Db)
                mul!(y, P, Db)
                @test all(isfinite, collect(y))
                if check_vec !== nothing
                    @test check_vec(_gpu_pc_chunk(y.chunks[1]))
                end
                if inner isa Dagger.DeviceILU0 && check_vec !== nothing
                    raw = _gpu_pc_chunk(Db.chunks[1])
                    if raw isa Dagger.Chunk && Dagger.root_worker_id(raw) == myid()
                        @test Dagger._supports_device_apply(inner,
                            Dagger.MemPool.poolget(raw.handle))
                    end
                end
            end
        end
    end
end
