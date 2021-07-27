##
# This file is a part of Dagger.jl. License is MIT
#
# Based upon https://github.com/google/benchmark, which is licensed under Apache v2:
# https://github.com/google/benchmark/blob/master/LICENSE
#
# In compliance with the Apache v2 license, here are the original copyright notices:
# Copyright 2015 Google Inc. All rights reserved.
##

struct TimeSpec
    tv_sec  :: UInt64 # time_t
    tv_nsec :: UInt64
end

maketime(ts) = ts.tv_sec * UInt(1e9) + ts.tv_nsec

# From bits/times.h on a Linux system
# Check if those are the same on BSD
if Sys.islinux()
    const CLOCK_MONOTONIC          = Cint(1)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(2)
    const CLOCK_THREAD_CPUTIME_ID  = Cint(3)
elseif Sys.isfreebsd() # atleast on FreeBSD 11.1
    const CLOCK_MONOTONIC          = Cint(4)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(14)
elseif Sys.isapple() # Version 10.12 required
    const CLOCK_MONOTONIC          = Cint(6)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(12)
else
    @inline cputhreadtime() = time_ns()
    return
end

@inline function clock_gettime(cid)
    ts = Ref{TimeSpec}()
    ccall(:clock_gettime, Cint, (Cint, Ref{TimeSpec}), cid, ts)
    return ts[]
end

@inline function realtime()
    maketime(clock_gettime(CLOCK_MONOTONIC))
end

@inline function cputime()
    maketime(clock_gettime(CLOCK_PROCESS_CPUTIME_ID))
end

@static if Sys.islinux()
@inline function cputhreadtime()
    maketime(clock_gettime(CLOCK_THREAD_CPUTIME_ID))
end
else
@inline cputhreadtime() = time_ns()
end
