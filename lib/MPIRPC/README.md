# MPIRPC.jl

Standalone MPI-backed RPC for Julia, modeled after the `Distributed` stdlib remote
invocation pipeline (header + serialized body + boundary, `invokelatest` on the
handler path, `RemoteException`-style error wrapping) and Dagger's
"accelerate-once" backend selection (a single backend value held in a task-local
slot determines uniform vs. non-uniform semantics; the public API is the same in
both modes).

* Public API mirrors `Distributed`: `remotecall`, `remotecall_fetch`,
  `remotecall_wait`, `remote_do`, `bcast_remotecall`, `fetch`, `wait`, with an `MPIFuture` handle.
* Two backends:
  * `UniformMPIRPCBackend`: SPMD, every rank both issues and services RPC,
    every rank calls `rpc_progress!`.
  * `NonUniformMPIRPCBackend`: explicit listener / client roles, optional
    `MPI.Comm_split` to isolate RPC from world collectives, only listeners must
    poll `rpc_progress!`.
* Transport: `MPI.jl` only. No TCP, no `Distributed`, no `Dagger` dependency.
* Multi-threaded out of the box: handlers dispatch on `Threads.@spawn`,
  so user closures that themselves spawn tasks and `fetch` them do not
  deadlock under `julia --threads=N`. Trade-off: handler execution is
  not FIFO across messages from the same source — use `remotecall_wait`
  (or `remotecall` + `fetch`) when you need a side effect committed
  before the next call. See `docs/ARCHITECTURE.md` §6.
* Optional progress daemon: pass `daemon = true` to either backend
  constructor and `select_mpi_rpc_backend!` will spawn a yield-only
  background task that drives `rpc_progress!` for you. The daemon is
  placed on Julia's `:interactive` threadpool (start Julia with
  `-t N,M`, `M >= 1`), so CPU-bound user code on the `:default` pool
  cannot starve the wire pump. Removes the "every rank must call
  `rpc_progress!`" requirement at the cost of ≈ 100% utilization of
  one OS thread (the daemon never sleeps). `shutdown!` joins the daemon
  cleanly. See `docs/ARCHITECTURE.md` §6.
* Idle waits cost zero CPU under `daemon = true`: `wait` / `fetch` on
  an `MPIFuture` park on a `Threads.Condition` and are woken by
  `deliver!` from the daemon's reply-dispatch path. Without the daemon
  (`daemon = false`), `wait` falls back to a yield-rate spin because
  the calling task is the only available progress driver.
* Minimum Julia: `1.9` (stable `task_local_storage`, `Base.invokelatest`).

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the design rationale, the
deadlock and ABBA discussion, and the side-by-side mapping to `Distributed` and
to Dagger's `accelerate!`.

## Quick start

```julia
using MPI, MPIRPC

MPI.Init(; threadlevel=:multiple)
MPIRPC.select_mpi_rpc_backend!(MPIRPC.UniformMPIRPCBackend(MPI.COMM_WORLD))

rank = MPI.Comm_rank(MPI.COMM_WORLD)
nprocs = MPI.Comm_size(MPI.COMM_WORLD)
peer = mod(rank + 1, nprocs)

result = MPIRPC.@with_progress MPIRPC.remotecall_fetch(+, peer, rank, 100)
@assert result == rank + 100  # the args (rank, 100) travel to `peer` which sums them

# Fan out the same call to every other rank; collect later:
futs = MPIRPC.bcast_remotecall(MPIRPC.current_mpi_rpc_backend(), *, rank, 7)
vals = map(MPIRPC.fetch, futs)  # one entry per destination rank (ascending), excluding `rank`

MPIRPC.shutdown!()
MPI.Finalize()
```

## Security

Like `Distributed`, MPIRPC deserializes function objects and arguments sent by
peers. The same trust model applies: only run MPIRPC across mutually trusted
ranks.
