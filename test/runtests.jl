addprocs(4)

if VERSION < v"0.5.0-dev"
    using BaseTestNext
else
    using Base.Test
    Pkg.checkout("Requires")
end

af_isinstalled = try Pkg.installed("ArrayFire") >= v"0.0.0-" catch er false end

if af_isinstalled
    try
        using ArrayFire
    catch err
        println(STDERR, "Failed to load ArrayFire. It is not installed properly")
        rethrow(err)
    end

    using ComputeFramework
    include("domain.jl")
    include("array.jl")
    include("gpu.jl")
else
    using ComputeFramework
    info("ArrayFire isn't installed. Skipping GPU tests")
    include("domain.jl")
    include("array.jl")
end

