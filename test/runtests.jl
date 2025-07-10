IN_CI = parse(Bool, get(ENV, "CI", "0"))

USE_CUDA = parse(Bool, get(ENV, "CI_USE_CUDA", "0"))
USE_ROCM = parse(Bool, get(ENV, "CI_USE_ROCM", "0"))
USE_ONEAPI = parse(Bool, get(ENV, "CI_USE_ONEAPI", "0"))
USE_METAL = parse(Bool, get(ENV, "CI_USE_METAL", "0"))
USE_OPENCL = parse(Bool, get(ENV, "CI_USE_OPENCL", "0"))
USE_GPU = USE_CUDA || USE_ROCM || USE_ONEAPI || USE_METAL || USE_OPENCL

# (title, filename, supports_gpu)
tests = [
    ("Thunk", "thunk.jl", false),
    ("Scheduler", "scheduler.jl", false),
    ("Processors", "processors.jl", false),
    ("Memory Spaces", "memory-spaces.jl", false),
    ("Logging", "logging.jl", false),
    ("Checkpointing", "checkpoint.jl", false),
    ("Scopes", "scopes.jl", false),
    ("Options", "options.jl", false),
    ("Mutation", "mutation.jl", false),
    ("Task Queues", "task-queues.jl", false),
    ("Task Affinity", "task-affinity.jl", false),
    ("Datadeps", "datadeps.jl", false),
    ("Streaming", "streaming.jl", false),
    ("Domain Utilities", "domain.jl", false),
    ("Array - Allocation", "array/allocation.jl", false),
    ("Array - Indexing", "array/indexing.jl", false),
    ("Array - Core", "array/core.jl", false),
    ("Array - Copyto", "array/copyto.jl", false),
    ("Array - MapReduce", "array/mapreduce.jl", false),
    ("Array - LinearAlgebra - Core", "array/linalg/core.jl", true),
    ("Array - LinearAlgebra - Matmul", "array/linalg/matmul.jl", true),
    ("Array - LinearAlgebra - Cholesky", "array/linalg/cholesky.jl", true),
    ("Array - LinearAlgebra - LU", "array/linalg/lu.jl", true),
    ("Array - Random", "array/random.jl", false),
    ("Array - Stencils", "array/stencil.jl", true),
    ("Array - FFT", "array/fft.jl", false),
    ("GPU", "gpu.jl", true),
    ("Caching", "cache.jl", false),
    ("Disk Caching", "diskcaching.jl", false),
    ("File IO", "file-io.jl", false),
    ("External Languages - Python", "extlang/python.jl", false),
    ("Preferences", "preferences.jl", false),
    #("Fault Tolerance", "fault-tolerance.jl", false),
]
if USE_GPU
    # Only run GPU tests
    filter!(test->test[3], tests)
end
all_test_names = map(test -> replace(test[2], ".jl"=>""), tests)

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
                nargs = '*'
                default = all_test_names
                help = "Enables the specified test to run in the testsuite"
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
    for test in parsed_args["test"]
        if isdir(joinpath(@__DIR__, test))
            for (_, other_test, _) in tests
                if startswith(other_test, test)
                    push!(to_test, other_test)
                    continue
                end
            end
        elseif test in all_test_names
            push!(to_test, test)
        else
            println(stderr, "Unknown test: $test")
            println(stderr, "Available tests:")
            for ((test_title, _, _), test_name) in zip(tests, all_test_names)
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

CPU_SCOPES = Tuple{Symbol, Type, Dagger.AbstractScope}[]
GPU_SCOPES = Tuple{Symbol, Type, Dagger.AbstractScope}[]
if USE_GPU
    include("setup_gpu.jl")
else
    push!(CPU_SCOPES, (:OneWorker_SingleThreaded, Array, Dagger.scope(;worker=1, thread=1)))
    push!(CPU_SCOPES, (:OneWorker_MultiThreaded, Array, Dagger.scope(;worker=1, threads=:)))
    push!(CPU_SCOPES, (:MultiWorker_SingleThreaded, Array, Dagger.scope(;workers=:, thread=1)))
    push!(CPU_SCOPES, (:MultiWorker_MultiThreaded, Array, Dagger.scope(;workers=:, threads=:)))
end
ALL_SCOPES = vcat(CPU_SCOPES, GPU_SCOPES)

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
