# RPC-based matrix multiplication (non-uniform: listeners own B, clients own A)
#
# First half of ranks are **listeners** (service RPC); the rest are **clients**
# (issue RPC only). Each listener owns one contiguous column strip of `B`;
# each client owns one contiguous row block of `A`. Clients fetch every column
# strip from the listener ranks via `remotecall_fetch` — clients never call
# each other (matches `NonUniformMPIRPCBackend` constraints).
#
# Run from the MPIRPC package root under lib/MPIRPC (needs at least 4 ranks, same as package
# non-uniform tests):
#   mpiexec -n 4 julia --threads=2,1 --project=. examples/rpc_matmul_nonuniform.jl
#
# Optional: `N=128` env sets matrix dimension (must satisfy divisibility below).
#
# We enable `daemon = true` so listeners continuously drain inbound RPC without
# a dedicated manual `serve_listener` loop while clients issue many fetches.

using MPI
using MPIRPC

const MAX_VERIFY_N = 256

a_elem(i::Int, j::Int) = sin(i) + cos(j)
b_elem(i::Int, j::Int) = tanh(i * 0.01) + tanh(j * 0.01)

const _RPC_B_STRIP = Ref{Matrix{Float64}}()

MPI.Init(; threadlevel = :multiple)

const WORLD_RANK = MPI.Comm_rank(MPI.COMM_WORLD)
const NPROC      = MPI.Comm_size(MPI.COMM_WORLD)

NPROC >= 4 || error("non-uniform matmul example needs at least 4 ranks (got $NPROC)")

const HALF           = max(1, NPROC ÷ 2)
const LISTENER_RANKS = collect(0:(HALF - 1))
const CLIENT_RANKS   = collect(HALF:(NPROC - 1))
const IS_LISTENER    = WORLD_RANK in LISTENER_RANKS

const N_LISTENERS = length(LISTENER_RANKS)
const N_CLIENTS   = length(CLIENT_RANKS)

backend = MPIRPC.select_mpi_rpc_backend!(
    NonUniformMPIRPCBackend(MPI.COMM_WORLD;
                            listener_ranks = LISTENER_RANKS,
                            daemon           = true))
const COMM = backend.comm
const RANK = MPI.Comm_rank(COMM)
@assert RANK == WORLD_RANK

const N = parse(Int, get(ENV, "N", "64"))
N % N_LISTENERS == 0 || error("N ($N) must be divisible by number of listeners ($N_LISTENERS)")
N % N_CLIENTS == 0 || error("N ($N) must be divisible by number of clients ($N_CLIENTS)")

const N_LOC_COL = N ÷ N_LISTENERS   # columns per listener strip
const N_LOC_ROW = N ÷ N_CLIENTS   # rows per client block

# --- Build owned pieces -------------------------------------------------------

if IS_LISTENER
    ℓ = findfirst(==(WORLD_RANK), LISTENER_RANKS)::Int
    COL_LO = (ℓ - 1) * N_LOC_COL + 1
    COL_HI = ℓ * N_LOC_COL
    B_loc = Matrix{Float64}(undef, N, N_LOC_COL)
    for lj in 1:N_LOC_COL
        j_g = COL_LO + lj - 1
        for i in 1:N
            B_loc[i, lj] = b_elem(i, j_g)
        end
    end
    _RPC_B_STRIP[] = B_loc
else
    c = findfirst(==(WORLD_RANK), CLIENT_RANKS)::Int
    ROW_LO = (c - 1) * N_LOC_ROW + 1
    ROW_HI = c * N_LOC_ROW
    A_loc = Matrix{Float64}(undef, N_LOC_ROW, N)
    for li in 1:N_LOC_ROW
        i_g = ROW_LO + li - 1
        for j in 1:N
            A_loc[li, j] = a_elem(i_g, j)
        end
    end
end

MPIRPC.rpc_barrier()

# --- Clients multiply; listeners only service RPC (daemon drives progress) -----

if IS_LISTENER
    C_loc = nothing
else
    C_loc = zeros(N_LOC_ROW, N)
    for ℓ in 1:N_LISTENERS
        listener = LISTENER_RANKS[ℓ]
        clo = (ℓ - 1) * N_LOC_COL + 1
        chi = ℓ * N_LOC_COL
        B_panel = MPIRPC.remotecall_fetch(listener) do
            return copy(Main._RPC_B_STRIP[])
        end
        size(B_panel) == (N, N_LOC_COL) || error("bad B_panel size from listener $listener")
        C_loc[:, clo:chi] = A_loc * B_panel
    end
end

MPIRPC.rpc_barrier()

# --- Verification on first client only (small N) ------------------------------

VERIFY_RANK = first(CLIENT_RANKS)
if WORLD_RANK == VERIFY_RANK && N ≤ MAX_VERIFY_N && !IS_LISTENER
    c = findfirst(==(WORLD_RANK), CLIENT_RANKS)::Int
    ROW_LO = (c - 1) * N_LOC_ROW + 1
    ROW_HI = c * N_LOC_ROW
    A_full = Matrix{Float64}(undef, N, N)
    B_full = Matrix{Float64}(undef, N, N)
    for j in 1:N, i in 1:N
        A_full[i, j] = a_elem(i, j)
        B_full[i, j] = b_elem(i, j)
    end
    C_ref = A_full * B_full
    ref_rows = C_ref[ROW_LO:ROW_HI, :]
    err = maximum(abs.(C_loc .- ref_rows))
    println("[client rank $WORLD_RANK] max |C_loc - C_ref| on owned rows = ", err)
    @assert err < 1e-10 * max(1.0, maximum(abs.(C_ref)))
elseif WORLD_RANK == VERIFY_RANK
    println("[client rank $WORLD_RANK] skipping dense verification (N=$N > $MAX_VERIFY_N)")
end

MPIRPC.rpc_barrier()
MPIRPC.shutdown!()
MPI.Finalize()
