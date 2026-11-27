# The affinity-mask helpers behind `allowed_cpus` and `pin_threads!` have no system calls in them, so they are tested on every
# platform, including masks for machines with more than 1024 CPUs, which no CI machine has.
@testset "Thread placement: affinity masks" begin
    # decoding: bit b of byte i (from 0) is CPU 8i + b
    @test isempty(Dagger._decode_cpu_mask(zeros(UInt8, 128)))
    buf = zeros(UInt8, 256)
    buf[1] = 0b0000_0101; buf[2] = 0b1000_0000; buf[129] = 0x01; buf[256] = 0x80
    @test Dagger._decode_cpu_mask(buf) == [0, 2, 15, 1024, 2047]
    # a mask with exactly one CPU set, in whole 8-byte words and with room for at least 1024 CPUs
    for cpu in (0, 1, 7, 8, 63, 64, 1023, 1024, 1031, 1032, 4095, 4096, 100_000)
        mask = Dagger._cpu_mask(cpu)
        @test length(mask) % 8 == 0 && length(mask) >= 128 && length(mask) * 8 > cpu
        @test Dagger._decode_cpu_mask(mask) == [cpu]
    end
    @test length(Dagger._cpu_mask(1023)) == 128 && length(Dagger._cpu_mask(1024)) == 136
    @test_throws ArgumentError Dagger._cpu_mask(-1)
    # `allowed_cpus` asks again with a bigger buffer until the kernel accepts the mask (it refuses one smaller than its CPU count)
    function machine(ncpus, allowed)
        asked = Int[]
        getaffinity = function (nbytes, buf)
            push!(asked, nbytes)
            length(asked) > 20 && error("allowed_cpus keeps asking with a mask of $nbytes bytes")   # bounded, whatever the code does
            nbytes * 8 < ncpus && return -1
            for c in allowed
                buf[(c >> 3) + 1] |= UInt8(1) << (c & 7)
            end
            return 0
        end
        return getaffinity, asked
    end
    get1, asked1 = machine(64, [0, 5, 63])
    @test Dagger._allowed_cpus(get1) == [0, 5, 63] && asked1 == [128]
    get2, asked2 = machine(4096, [0, 3, 2000, 4095])
    @test Dagger._allowed_cpus(get2) == [0, 3, 2000, 4095] && asked2 == [128, 256, 512]
    get3, _ = machine(300_000, [299_999, 5])
    @test Dagger._allowed_cpus(get3) == [5, 299_999]
    never = Ref(0)
    @test isempty(Dagger._allowed_cpus((nbytes, buf) -> ((never[] += 1) > 50 ? error("endless loop") : -1)))   # a kernel that never accepts: nothing, not an endless loop
    @test never[] == 10                                           # 128, 256, ..., 65536 bytes
    # which CPU each thread is pinned to
    @test Dagger._pin_targets([1, 2, 3], [4, 5, 6]) == [4, 5, 6]
    @test Dagger._pin_targets([1, 3], [7, 6, 5, -9]) == [7, 6]    # the CPUs beyond the threads are not used (nor checked)
    @test Dagger._pin_targets(Int[], Int[]) == Int[]
    @test_throws ArgumentError Dagger._pin_targets([1, 2, 3], [0, 1])
    @test_throws ArgumentError Dagger._pin_targets([1, 2], [0, -1])
end

@testset "Thread placement" begin
    cpus = Dagger.thread_cpus()
    if Sys.islinux()
        tids = [t for t in 1:Threads.maxthreadid() if Threads.threadpool(t) === :default]
        @test length(cpus) == Threads.maxthreadid()
        @test all(t -> cpus[t] >= 0, tids)
        allowed = Dagger.allowed_cpus()
        @test !isempty(allowed) && issorted(allowed)
        @test all(t -> cpus[t] in allowed, tids)                   # unpinned threads run where the process may run
        @test_throws ArgumentError Dagger.pin_threads!(; cpus=Int[])
        @test_throws ArgumentError Dagger.pin_threads!(; cpus=fill(-1, length(tids)))
        if length(allowed) >= length(tids)
            # pin onto the CPUs the launcher allowed, in order, and read the result back
            pinned = Dagger.pin_threads!()
            @test [pinned[t] for t in tids] == allowed[1:length(tids)]
            @test [Dagger.thread_cpus()[t] for t in tids] == allowed[1:length(tids)]
            # an explicit list is used as given, in thread order, and the result is what thread_cpus reports
            custom = reverse(allowed[1:length(tids)])
            @test [Dagger.pin_threads!(; cpus=custom)[t] for t in tids] == custom
            # a CPU the kernel refuses leaves that thread where it was, and is reported
            @test_logs (:warn, "Could not pin threads") match_mode=:any Dagger.pin_threads!(; cpus=fill(10^6, length(tids)))
            Dagger.pin_threads!()
        end
    else
        @test isempty(cpus)
        @test isempty(Dagger.allowed_cpus()) && isempty(Dagger.pin_threads!())
        @test isempty(Dagger.pin_threads!(; cpus=Int[]))           # nothing to do off Linux, whatever the list
    end
end
