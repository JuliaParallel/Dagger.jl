<img src="docs/src/assets/logo-with-text.jpg" alt="Dagger.jl">

-----

<h2 align="center">A framework for out-of-core and parallel computing</h2>

| **Documentation**                       | **Build Status**                        |
|:---------------------------------------:|:---------------------------------------:|
| [![][docs-master-img]][docs-master-url] | [![Build Status][build-img]][build-url] |

[docs-master-img]: https://img.shields.io/badge/docs-master-blue.svg
[docs-master-url]: https://juliaparallel.github.io/Dagger.jl/dev
[build-img]: https://badge.buildkite.com/d8f020afb67a5920709c2b0a29111cf596f3f052099b5b656f.svg?branch=master
[build-url]: https://buildkite.com/julialang/dagger-dot-jl

At the core of Dagger.jl is a scheduler heavily inspired by [Dask](https://docs.dask.org/en/latest/). It can run computations represented as [directed-acyclic-graphs](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAGs) efficiently on many Julia worker processes and threads, as well as GPUs via [DaggerGPU.jl](https://github.com/JuliaGPU/DaggerGPU.jl).

*The **DTable** has been moved out of this repository. You can now find it [here](https://github.com/JuliaParallel/DTables.jl).*

## Installation

Dagger.jl can be installed using the Julia package manager. Enter the Pkg REPL
mode by typing "]" in the Julia REPL and then run:

```julia
pkg> add Dagger
```

Or, equivalently, install Dagger via the Pkg API:

```julia
julia> import Pkg; Pkg.add("Dagger")
```

## Usage

Once installed, the `Dagger` package can be loaded with `using Dagger`, or if
you want to use Dagger for distributed computing, it can be loaded as:

```julia
using Distributed; addprocs() # Add one Julia worker per CPU core
using Dagger
```

You can run the following example to see how Dagger exposes easy parallelism:

```julia
# This runs first:
a = Dagger.@spawn rand(100, 100)

# These run in parallel:
b = Dagger.@spawn sum(a)
c = Dagger.@spawn prod(a)

# Finally, this runs:
wait(Dagger.@spawn println("b: ", b, ", c: ", c))
```

## Use Cases

Dagger can support a variety of use cases that benefit from easy, automatic
parallelism, such as:

- [Parallelizing Nested Loops](https://juliaparallel.org/Dagger.jl/dev/use-cases/parallel-nested-loops/#Use-Case:-Parallel-Nested-Loops)

This isn't an exhaustive list of the use cases that Dagger supports. There are
more examples in the docs, and more use cases examples are welcome (just file
an issue or PR).

## Contributing Guide

Please see the roadmap for missing features or known bugs:

[Dagger Features and Roadmap](FEATURES_ROADMAP.md)

Other resources:

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
[![GitHub issues](https://img.shields.io/github/issues/JuliaParallel/Dagger.jl)](https://github.com/JuliaParallel/Dagger.jl/issues)
[![GitHub contributors](https://img.shields.io/github/contributors/JuliaParallel/Dagger.jl)](https://github.com/JuliaParallel/Dagger.jl/graphs/contributors)

Contributions are encouraged.

There are several ways to contribute to our project:

**Reporting Bugs**: If you find a bug, please open an issue and describe the problem. Make sure to include steps to reproduce the issue and any error messages you receive regarding that issue.

**Fixing Bugs**: If you'd like to fix a bug, please create a pull request with your changes. Make sure to include a description of the problem and how your changes will address it.

Additional examples and documentation improvements are also very welcome.

## Resources

List of recommended Dagger.jl resources:
- Docs [![][docs-master-img]][docs-master-url]
- Videos
  - [Distributed Computing with Dagger.jl](https://youtu.be/capjmjVHfMU)
  - [Easy, Featureful Parallelism with Dagger.jl](https://youtu.be/t3S8W6A4Ago)
  - [Easier parallel Julia workflow with Dagger.jl](https://youtu.be/VrqzOsav61w)
  - [Dagger.jl Development and Roadmap](https://youtu.be/G0Y62ysFbDk)

## Help and Discussion
For help and discussion, we suggest asking in the following places:

[Julia Discourse](https://discourse.julialang.org/c/domain/parallel/34) and on the [Julia Slack](https://julialang.org/slack/) in the `#dagger` channel.

## References
```bibtex
@article{dagger1,
  title={{{Dynamic Task Scheduling with Data Dependency Awareness Using Julia}},
  author={Alomairy, Rabab and Tome, Felipe and Samaroo, Julian and Edelman, Alan},
  pages={1--6},
  year={2024},
  publisher={MIT Open Access Articles}
}
```
```bibtex
@article{dagger2,
  title={{{Efficient Dynamic Task Scheduling in Heterogeneous Environments with Julia}},
  author={Samaroo, Julian and Alomairy, Rabab and  and Giordano, Mose and Edelman, Alan},
  year={2024},
  publisher={MIT Open Access Articles}
}
```

## Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
