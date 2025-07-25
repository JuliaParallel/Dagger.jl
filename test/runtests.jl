IN_CI = parse(Bool, get(ENV, "CI", "0"))

USE_CUDA = parse(Bool, get(ENV, "CI_USE_CUDA", "0"))
USE_ROCM = parse(Bool, get(ENV, "CI_USE_ROCM", "0"))
USE_ONEAPI = parse(Bool, get(ENV, "CI_USE_ONEAPI", "0"))
USE_METAL = parse(Bool, get(ENV, "CI_USE_METAL", "0"))
USE_OPENCL = parse(Bool, get(ENV, "CI_USE_OPENCL", "0"))
USE_GPU = USE_CUDA || USE_ROCM || USE_ONEAPI || USE_METAL || USE_OPENCL

tests = [
    ("Thunk", "thunk.jl"),
    ("Scheduler", "scheduler.jl"),
    ("Processors", "processors.jl"),
    ("Memory Spaces", "memory-spaces.jl"),
    ("Logging", "logging.jl"),
    ("Checkpointing", "checkpoint.jl"),
    ("Scopes", "scopes.jl"),
    ("Options", "options.jl"),
    ("Mutation", "mutation.jl"),
    ("Task Queues", "task-queues.jl"),
    ("Task Affinity", "task-affinity.jl"),
    ("Datadeps", "datadeps.jl"),
    ("Streaming", "streaming.jl"),
    ("Domain Utilities", "domain.jl"),
    ("Array - Allocation", "array/allocation.jl"),
    ("Array - Indexing", "array/indexing.jl"),
    ("Array - Core", "array/core.jl"),
    ("Array - Copyto", "array/copyto.jl"),
    ("Array - MapReduce", "array/mapreduce.jl"),
    ("Array - LinearAlgebra - Core", "array/linalg/core.jl"),
    ("Array - LinearAlgebra - Matmul", "array/linalg/matmul.jl"),
    ("Array - LinearAlgebra - Cholesky", "array/linalg/cholesky.jl"),
    ("Array - LinearAlgebra - LU", "array/linalg/lu.jl"),
    ("Array - Random", "array/random.jl"),
    ("Array - Stencils", "array/stencil.jl"),
    ("Array - FFT", "array/fft.jl"),
    ("GPU", "gpu.jl"),
    #("Caching", "cache.jl"),
    ("Disk Caching", "diskcaching.jl"),
    ("File IO", "file-io.jl"),
    ("Reusable Data Structures", "reuse.jl"),
    ("External Languages - Python", "extlang/python.jl"),
    ("Preferences", "preferences.jl"),
    #("Fault Tolerance", "fault-tolerance.jl"),
]
if USE_GPU
    # Only run GPU tests
    tests = [
        ("GPU", "gpu.jl"),
        ("Array - Stencils", "array/stencil.jl"),
    ]
end
all_test_names = map(test -> replace(last(test), ".jl"=>""), tests)

additional_workers::Int = 3

if PROGRAM_FILE != "" && realpath(PROGRAM_FILE) == @__FILE__
    pushfirst!(LOAD_PATH, @__DIR__)
    pushfirst!(LOAD_PATH, joinpath(@__DIR__, ".."))
    using Pkg
    Pkg.activate(@__DIR__)
    try
        Pkg.instantiate()
    catch
    end

    using ArgParse
    s = ArgParseSettings(description = "Dagger Testsuite")
    @eval begin
        @add_arg_table! s begin
            "--test"
                nargs = 1
                action = :append_arg
                arg_type = String
                help = "Enables the specified test to run in the testsuite"
            "--no-test"
                nargs = 1
                action = :append_arg
                arg_type = String
                help = "Disables the specified test from running in the testsuite"
            "-s", "--simulate"
                action = :store_true
                help = "Don't actually run the tests"
            "-p", "--procs"
                arg_type = Int
                default = additional_workers
                help = "How many additional workers to launch"
            "-v", "--verbose"
                action = :store_true
                help = "Run the tests with debug logs from Dagger"
            "-O", "--offline"
                action = :store_true
                help = "Set Pkg into offline mode"
        end
    end

    parsed_args = parse_args(s)
    to_test = String[]
    if isempty(parsed_args["test"])
        to_test = copy(all_test_names)
    else
        for _test in parsed_args["test"]
            test = only(_test)
            if isdir(joinpath(@__DIR__, test))
                for (_, other_test) in tests
                    if startswith(other_test, test)
                        push!(to_test, other_test)
                    end
                end
            elseif test in all_test_names
                push!(to_test, test)
            else
                println(stderr, "Unknown test: $test")
                println(stderr, "Available tests:")
                for ((test_title, _), test_name) in zip(tests, all_test_names)
                    println(stderr, "  $test_name: $test_title")
                end
                exit(1)
            end
        end
    end
    for _test in parsed_args["no-test"]
        test = only(_test)
        if isdir(joinpath(@__DIR__, test))
            for (_, other_test) in tests
                if startswith(other_test, test)
                    filter!(x -> x != other_test, to_test)
                end
            end
        elseif test in all_test_names
            filter!(x -> x != test, to_test)
        else
            println(stderr, "Unknown test: $test")
            println(stderr, "Available tests:")
            for ((test_title, _), test_name) in zip(tests, all_test_names)
                println(stderr, "  $test_name: $test_title")
            end
            exit(1)
        end
    end

    @info "Running tests: $(join(to_test, ", "))"
    parsed_args["simulate"] && exit(0)

    additional_workers = parsed_args["procs"]

    if parsed_args["verbose"]
        ENV["JULIA_DEBUG"] = "Dagger"
    end

    if parsed_args["offline"]
        Pkg.UPDATED_REGISTRY_THIS_SESSION[] = true
        Pkg.offline(true)
    end
else
    to_test = all_test_names
    @info "Running all tests"
end

using Distributed
if additional_workers > 0
    # We put this inside a branch because addprocs() takes a minimum of 1s to
    # complete even if doing nothing, which is annoying.
    addprocs(additional_workers; exeflags="--project=$(joinpath(@__DIR__, ".."))")
end

include("imports.jl")
include("util.jl")
include("fakeproc.jl")

using Test
using Dagger
using UUIDs
@everywhere import MemPool

@everywhere MemPool.MEM_RESERVED[] = 0

GPU_SCOPES = Pair{Symbol, Dagger.AbstractScope}[]
if USE_GPU
    include("setup_gpu.jl")
end

try
    for test in to_test
        test_title = tests[findfirst(x->x[2]==test * ".jl", tests)][1]
        test_name = all_test_names[findfirst(x->x==test, all_test_names)]
        println()
        @info "Testing $test_title ($test_name)"
        @testset "$test_title" include(test * ".jl")
    end
catch
    printstyled(stderr, "Tests Failed!\n"; color=:red)
    rethrow()
finally
    state = Dagger.Sch.EAGER_STATE[]
    if state !== nothing
        notify(state.halt)
    end
    sleep(1)
    if nprocs() > 1
        rmprocs(workers())
    end
end

printstyled(stderr, "Tests Completed!\n"; color=:green)
