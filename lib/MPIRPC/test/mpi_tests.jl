using Test
using MPI

# Orchestrate the two mpiexec-driven suites from the host test process.
# `Pkg.test()` runs in a single process; we spawn `mpiexec` here so the
# usual `julia --project=. -e 'using Pkg; Pkg.test()'` flow exercises the
# real MPI transport.

const MPI_BIN = mpiexec()

const HERE = @__DIR__
const PROJECT = abspath(joinpath(HERE, ".."))

function _run_mpi(script::AbstractString; nproc::Integer,
                  default_threads::Integer = 2,
                  interactive_threads::Integer = 1)
    # Pass `--threads=N,M` to each spawned Julia: N default-pool threads
    # to exercise concurrent handlers and concurrent client tasks, plus M
    # interactive-pool threads so the MPIRPC daemon (when enabled) can be
    # scheduled on its own pool, isolated from any user CPU-bound work
    # running on the default pool.
    #
    # `default_threads = 2` is the minimum that turns `Threads.@spawn`
    # into actual parallelism. `interactive_threads = 1` is the minimum
    # that gives the daemon a dedicated thread; the daemon-suite scripts
    # additionally assert that they are running with the daemon on
    # `:interactive`, so this needs to stay >= 1.
    threadspec = "$(default_threads),$(interactive_threads)"
    cmd = `$(MPI_BIN) -n $(nproc) $(Base.julia_cmd()) --threads=$(threadspec) --project=$(PROJECT) $(script)`
    @info "MPIRPC: running" script nproc default_threads interactive_threads cmd
    rc = success(pipeline(cmd, stdout = stdout, stderr = stderr))
    return rc
end

@testset "mpiexec / uniform backend (4 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec uniform suite (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        @test _run_mpi(joinpath(HERE, "uniform_mpiexec.jl"); nproc = 4)
    end
end

@testset "mpiexec / non-uniform backend (4 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec non-uniform suite (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        @test _run_mpi(joinpath(HERE, "nonuniform_mpiexec.jl"); nproc = 4)
    end
end

# The two daemon suites verify the optional progress-daemon path: the
# scripts never call `rpc_progress!` or `serve_listener` themselves, so a
# successful run is *only* possible if the yield-only `_daemon_loop`
# spawned by `select_mpi_rpc_backend!` is doing its job. We give them a
# separate testset (rather than folding them into the existing two) so a
# regression in the daemon does not get masked by the manual-pump suites.
@testset "mpiexec / uniform backend with daemon (4 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec uniform-daemon suite (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        @test _run_mpi(joinpath(HERE, "uniform_daemon_mpiexec.jl"); nproc = 4)
    end
end

@testset "mpiexec / non-uniform backend with daemon (4 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec non-uniform-daemon suite (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        @test _run_mpi(joinpath(HERE, "nonuniform_daemon_mpiexec.jl"); nproc = 4)
    end
end

# Example: RPC matmul with explicit listener / client roles (no client-to-client RPC).
@testset "mpiexec / bcast_remotecall (3 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec bcast_remotecall (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        @test _run_mpi(joinpath(HERE, "bcast_remotecall_mpiexec.jl"); nproc = 3)
    end
end

# Example: RPC matmul with explicit listener / client roles (no client-to-client RPC).
@testset "mpiexec / example rpc_matmul non-uniform (4 ranks)" begin
    if get(ENV, "MPIRPC_SKIP_MPI_TESTS", "0") == "1"
        @info "MPIRPC: skipping mpiexec rpc_matmul_nonuniform (MPIRPC_SKIP_MPI_TESTS=1)"
        @test true
    else
        ex = joinpath(dirname(HERE), "examples", "rpc_matmul_nonuniform.jl")
        @test _run_mpi(ex; nproc = 4)
    end
end
