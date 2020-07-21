# Dagger.jl

*A framework for out-of-core and parallel computing*

| **Documentation**                                                       | **Build Status**                                              |
|:---------------------------------------:|:-------------------------------------------------------------:|
| [![][docs-master-img]][docs-master-url] | [![Build Status](https://travis-ci.org/JuliaParallel/Dagger.jl.svg?branch=master)](https://travis-ci.org/JuliaParallel/Dagger.jl) [![Coverage Status](https://coveralls.io/repos/github/JuliaParallel/Dagger.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaParallel/Dagger.jl?branch=master) |

[docs-master-img]: https://img.shields.io/badge/docs-master-blue.svg
[docs-master-url]: https://juliaparallel.github.io/Dagger.jl/dev

At the core of Dagger.jl is a scheduler heavily inspired by [Dask](https://docs.dask.org/en/latest/). It can run computations represented as [directed-acyclic-graphs](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAGs) efficiently on many Julia worker processes.


## Installation

You can install Dagger by typing

```julia
julia> ] add Dagger
```

## Usage

Once installed, the `Dagger` package can by used by typing

```julia
using Dagger
```

## Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
