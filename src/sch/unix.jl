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

maketime(ts) = ts.tv_sec * 1e9 + ts.tv_nsec

# From bits/times.h on a Linux system
# Check if those are the same on BSD
if Sys.islinux()
    const CLOCK_MONOTONIC          = Cint(1)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(2)
    const CLOCK_THREAD_CPUTIME_ID  = Cint(3)
elseif Sys.KERNEL == :FreeBSD # atleast on FreeBSD 11.1
    const CLOCK_MONOTONIC          = Cint(4)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(14)
elseif Compat.Sys.isapple() # Version 10.12 required
    const CLOCK_MONOTONIC          = Cint(6)
    const CLOCK_PROCESS_CPUTIME_ID = Cint(12)
else
    error("""
          BenchmarkTools doesn't currently support your operating system.
          Please file an issue, your kernel is $(Sys.KERNEL)
          """)
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

@inline function cputhreadtime()
    maketime(clock_gettime(CLOCK_THREAD_CPUTIME_ID))
end

struct Measurement
    realtime::TimeSpec
    cputime::TimeSpec
    function Measurement()
        rtime = clock_gettime(CLOCK_MONOTONIC)
        ctime = clock_gettime(CLOCK_PROCESS_CPUTIME_ID)
        return new(rtime, ctime)
    end
end

struct MeasurementDelta
    realtime::Float64
    cpuratio::Float64
    function MeasurementDelta(t1::Measurement, t0::Measurement)
        rt0 = maketime(t0.realtime)
        ct0 = maketime(t0.cputime)
        rt1 = maketime(t1.realtime)
        ct1 = maketime(t1.cputime)
        realtime = rt1 - rt0
        cputime = ct1 - ct0
        return new(realtime, cputime/realtime)
    end
end
