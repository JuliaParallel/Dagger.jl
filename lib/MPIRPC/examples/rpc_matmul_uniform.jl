# RPC-based matrix multiplication (uniform SPMD)
#
# Each rank owns a row block of `A` and a column block of `B`. To form
# `C = A * B`, rank `r` needs every column strip of `B`; those strips live on
# distinct ranks, so we fetch each peer's strip via `remotecall_fetch`. No MPI
# collectives move `A` / `B` on the compute path — only point-to-point RPC.
#
# Run from the MPIRPC package root (e.g. Dagger.jl/lib/MPIRPC):
#   mpiexec -n 4 julia --project=. examples/rpc_matmul_uniform.jl
#
# Optional threading (matches package tests; useful if you switch to
# `UniformMPIRPCBackend(...; daemon = true)` — daemon uses `:interactive`):
#   mpiexec -n 4 julia --threads=2,1 --project=. examples/rpc_matmul_uniform.jl
#
# Limitations:
# * Matrix dimension `N` below must divide `NPROC`; keep `N` modest — each RPC
#   returns an `n × (N/nproc)` Float64 strip (multi-MiB if `N` is huge).
# * Rank-0 verification builds dense `N×N` references and only runs when
#   `N ≤ MAX_VERIFY_N` (memory + time).

using MPI
using MPIRPC

const MAX_VERIFY_N = 256

# Deterministic element generators (global indices i, j ∈ 1:N).
a_elem(i::Int, j::Int) = sin(i) + cos(j)
b_elem(i::Int, j::Int) = tanh(i * 0.01) + tanh(j * 0.01)

# Filled before any RPC so the handler closure on the destination rank reads the
# correct strip via `Main._RPC_B_STRIP` (each MPI rank is its own process).
const _RPC_B_STRIP = Ref{Matrix{Float64}}()

MPI.Init(; threadlevel = :multiple)
backend = MPIRPC.select_mpi_rpc_backend!(UniformMPIRPCBackend(MPI.COMM_WORLD))
const COMM  = backend.comm
const RANK  = MPI.Comm_rank(COMM)
const NPROC = MPI.Comm_size(COMM)

# Demo size: override with `N=128 julia ...` if desired (must divide world size).
const N = parse(Int, get(ENV, "N", "64"))
NPROC >= 1 || error("need at least 1 rank")
N % NPROC == 0 || error("N ($N) must be divisible by NPROC ($NPROC)")
const N_LOC = N ÷ NPROC

# --- Local owned data ---------------------------------------------------------

const ROW_LO = RANK * N_LOC + 1
const ROW_HI = (RANK + 1) * N_LOC
const COL_LO = RANK * N_LOC + 1
const COL_HI = (RANK + 1) * N_LOC

A_loc = Matrix{Float64}(undef, N_LOC, N)
B_loc = Matrix{Float64}(undef, N, N_LOC)

for li in 1:N_LOC
    i_g = ROW_LO + li - 1
    for j in 1:N
        A_loc[li, j] = a_elem(i_g, j)
    end
end
for lj in 1:N_LOC
    j_g = COL_LO + lj - 1
    for i in 1:N
        B_loc[i, lj] = b_elem(i, j_g)
    end
end

_RPC_B_STRIP[] = B_loc

MPIRPC.rpc_barrier()

# --- Compute C_loc = A_loc * B via RPC-fetched column strips -----------------

C_loc = zeros(N_LOC, N)

for q in 0:(NPROC - 1)
    clo = q * N_LOC + 1
    chi = (q + 1) * N_LOC
    # Closure runs on rank `q`; it captures nothing from rank `r` except what
    # Julia serializes — here we only need rank q's Main._RPC_B_STRIP.
    B_panel = MPIRPC.remotecall_fetch(q) do
        return copy(Main._RPC_B_STRIP[])
    end
    size(B_panel) == (N, N_LOC) || error("unexpected B_panel size on rank $RANK from $q")
    C_loc[:, clo:chi] = A_loc * B_panel
end

MPIRPC.rpc_barrier()

# --- Verification on rank 0 (small N only) ------------------------------------

if RANK == 0 && N ≤ MAX_VERIFY_N
    A_full = Matrix{Float64}(undef, N, N)
    B_full = Matrix{Float64}(undef, N, N)
    for j in 1:N, i in 1:N
        A_full[i, j] = a_elem(i, j)
        B_full[i, j] = b_elem(i, j)
    end
    C_ref = A_full * B_full
    ref_rows = C_ref[ROW_LO:ROW_HI, :]
    err = maximum(abs.(C_loc .- ref_rows))
    println("[rank 0] max |C_loc - C_ref| on owned rows = ", err)
    @assert err < 1e-10 * max(1.0, maximum(abs.(C_ref)))
elseif RANK == 0
    println("[rank 0] skipping dense verification (N=$N > MAX_VERIFY_N=$MAX_VERIFY_N)")
end

MPIRPC.rpc_barrier()
MPIRPC.shutdown!()
MPI.Finalize()
