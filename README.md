# Dagger.jl

*A framework for out-of-core and parallel computing*

| **Documentation**                       | **Build Status**                        |
|:---------------------------------------:|:---------------------------------------:|
| [![][docs-master-img]][docs-master-url] | [![Build Status][build-img]][build-url] |

[docs-master-img]: https://img.shields.io/badge/docs-master-blue.svg
[docs-master-url]: https://juliaparallel.github.io/Dagger.jl/dev
[build-img]: https://badge.buildkite.com/d8f020afb67a5920709c2b0a29111cf596f3f052099b5b656f.svg?branch=master
[build-url]: https://buildkite.com/julialang/dagger-dot-jl

At the core of Dagger.jl is a scheduler heavily inspired by [Dask](https://docs.dask.org/en/latest/). It can run computations represented as [directed-acyclic-graphs](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAGs) efficiently on many Julia worker processes and threads, as well as GPUs via [DaggerGPU.jl](https://github.com/JuliaGPU/DaggerGPU.jl).

***DTable** has been moved out of this repository. You can find the DTable in a standalone package format [here](https://github.com/JuliaParallel/DTables.jl).*

## Installation

You can install Dagger by typing

```julia
julia> ] add Dagger
```

## Usage

Once installed, the `Dagger` package can by used like so

```julia
using Distributed; addprocs() # get us some workers
using Dagger

# do some stuff in parallel!
a = Dagger.@spawn 1+3
b = Dagger.@spawn rand(a, 4)
c = Dagger.@spawn sum(b)
fetch(c) # some number!
```

## Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
