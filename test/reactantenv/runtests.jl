# Reactant integration tests. These live in their own project (and their own CI
# job), because Reactant is a heavy dependency to build and load.
#
# Run with:
#     julia --project=test/reactantenv test/reactantenv/runtests.jl

using Test
using Dagger
using Reactant
using LinearAlgebra
using Random
import Logging

import Dagger: @stencil, Wrap

# XLA's GPU backend is not what is under test here, and CI has no GPU
Reactant.set_default_backend("cpu")

@test Dagger.reactant_available()

include("inner.jl")
include("full.jl")
