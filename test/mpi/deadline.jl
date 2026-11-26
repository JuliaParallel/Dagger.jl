# `within(secs, what) do ... end` runs the block and, if it has not returned after `secs` seconds,
# reports the failure and aborts the job: a block that hangs in a collective cannot be recovered
# (the other ranks wait for it), so a hang has to become a failure with a message instead.
function within(f, secs::Real, what::AbstractString)
    done = Threads.Atomic{Bool}(false)
    watchdog = Threads.@spawn begin
        if timedwait(() -> done[], float(secs); pollint=0.05) !== :ok
            Core.println("[$(MPI.Comm_rank(MPI.COMM_WORLD))] Test Failed: TIMEOUT after $(secs) s in: $what")
            MPI.Abort(MPI.COMM_WORLD, 1)
        end
    end
    try
        return f()
    finally
        done[] = true
        wait(watchdog)
    end
end
