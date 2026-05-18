"""
    MPIRemoteException(rank::Int, captured::CapturedException)

Local analogue of `Distributed.RemoteException`. Wraps the originating MPI
rank and a `Base.CapturedException` for the original error and its captured
backtrace, so end-users can inspect the remote failure without any
`Distributed` dependency.
"""
struct MPIRemoteException <: Exception
    rank::Int
    captured::CapturedException
end

MPIRemoteException(captured::CapturedException) = MPIRemoteException(-1, captured)

Base.capture_exception(ex::MPIRemoteException, _) = ex

function Base.showerror(io::IO, re::MPIRemoteException)
    print(io, "On MPI rank ", re.rank, ":\n")
    showerror(io, re.captured)
end

"""
    run_work_thunk(thunk, rank; print_error::Bool=false) -> result_or_exception

Execute `thunk()` and either return the value or wrap any thrown exception
in an `MPIRemoteException`. When `print_error` is true (as for
`Distributed.remote_do`), the captured exception is also printed to stderr,
mirroring `Distributed.run_work_thunk(thunk, print_error)`.
"""
function run_work_thunk(thunk::Function, rank::Int; print_error::Bool=false)
    try
        return thunk()
    catch err
        ce = CapturedException(err, catch_backtrace())
        print_error && showerror(stderr, ce)
        return MPIRemoteException(rank, ce)
    end
end
