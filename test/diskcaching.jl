# N.B. Each child process gets its output captured to a file and is killed if it
# blows a generous deadline, rather than inheriting the testsuite's stdout and
# being waited on forever. When a child dies inside the Julia runtime it prints
# an unbounded object-graph dump before aborting: with stdout inherited, one
# such crash wrote ~13 GB into the test log and then spun at 100% CPU
# indefinitely, hanging the entire run and filling the machine's disk. Capturing
# the output bounds the damage, and echoing a truncated copy on failure keeps
# the crash diagnosable.
const DISKCACHING_CHILD_TIMEOUT = 900

function run_cache_setup_child(jl::Cmd, script::String, timeout::Real)
    logfile = tempname()
    proc = run(pipeline(`$jl $script`; stdout=logfile, stderr=logfile, append=true);
               wait=false)
    deadline = time() + timeout
    while process_running(proc) && time() < deadline
        sleep(0.5)
    end
    timed_out = process_running(proc)
    if timed_out
        kill(proc, Base.SIGKILL)
    end
    wait(proc)
    output = isfile(logfile) ? read(logfile, String) : ""
    rm(logfile; force=true)
    return (; timed_out, exitcode=proc.exitcode, output)
end

function report_cache_setup_failure(nprocs, result)
    output = result.output
    if length(output) > 8192
        output = string(first(output, 4096),
                        "\n[... output truncated ...]\n",
                        last(output, 4096))
    end
    @warn("cache_setup_test.jl failed",
          nprocs, result.timed_out, result.exitcode, output)
end

@testset "Disk caching setup on multiple processes (single machine)" begin
    script = joinpath(@__DIR__, "cache_setup_test.jl")
    for p in 0:3
        j = if p == 0
            Cmd(`julia --startup-file=no`)
        else
            Cmd(`julia --startup-file=no -p $p`)
        end
        result = withenv("JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR"=>nothing,
                         "JULIA_MEMPOOL_EXPERIMENTAL_MEMORY_BOUND"=>nothing,
                         "JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE"=>nothing,
                         "JULIA_MEMPOOL_EXPERIMENTAL_DISK_BOUND"=>nothing,
                         "JULIA_MEMPOOL_EXPERIMENTAL_ALLOCATOR_KIND"=>nothing) do
            run_cache_setup_child(j, script, DISKCACHING_CHILD_TIMEOUT)
        end
        if result.timed_out || result.exitcode != 0
            report_cache_setup_failure(p, result)
        end
        @test !result.timed_out
        @test result.exitcode == 0
    end
end
