"""
    MPIRPC

Standalone MPI-backed RPC for Julia, modeled on the `Distributed` stdlib
(`MsgHeader` + serialized body + boundary, `invokelatest` on the handler
path, `RemoteException`-style error wrapping) and on Dagger's
"accelerate-once" backend selection (a single backend value held in a
task-local slot determines uniform vs. non-uniform semantics; the public
API is the same in both modes).

See `docs/ARCHITECTURE.md` for the design narrative, the deadlock and
ABBA discussion, and the side-by-side mapping to `Distributed` and Dagger.
"""
module MPIRPC

using Serialization
using MPI

export AbstractMPIRPCBackend,
       UniformMPIRPCBackend,
       NonUniformMPIRPCBackend,
       MPIFuture,
       MPIRRID,
       MPIRemoteException,
       select_mpi_rpc_backend!,
       current_mpi_rpc_backend,
       initialize_mpi_rpc!,
       with_mpi_rpc_backend,
       shutdown!,
       remotecall,
       remotecall_fetch,
       remotecall_wait,
       remote_do,
       bcast_remotecall,
       rpc_progress!,
       rpc_progress_halt!,
       rpc_barrier,
       serve_listener,
       is_listener,
       listener_ranks,
       @with_progress

include("protocol.jl")
include("exceptions.jl")
include("config.jl")
include("refs.jl")
include("dispatch.jl")
include("uniform.jl")
include("nonuniform.jl")
include("remotecall.jl")
include("progress.jl")

end # module MPIRPC
