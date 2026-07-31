# Benchmarking Dagger

For ease of benchmarking changes to Dagger's scheduler and the DArray, a benchmarking script exists at `benchmarks/benchmark.jl`. This script currently allows benchmarking a non-negative matrix factorization (NNMF) algorithm, which we've found to be a good evaluator of scheduling performance. The benchmark script can test with and without Dagger, and also has support for using CUDA or AMD GPUs to accelerate the NNMF via DaggerGPU.jl.

The script checks for a number of environment variables, which are used to control the benchmarks that are performed (all of which are optional):

- `BENCHMARK_PROCS`: Selects the number of Julia processes and threads to start-up. Specified as `8:4`, this option would start 8 extra Julia processes, with 4 threads each. Defaults to 2 processors and 1 thread each.
- `BENCHMARK_REMOTES`: Specifies a colon-separated list of remote servers to connect to and start Julia processes on, using `BENCHMARK_PROCS` to indicate the processor/thread configuration of those remotes. Disabled by default (uses the local machine).
- `BENCHMARK_OUTPUT_FORMAT`: Selects the output format for benchmark results. Defaults to `jls`, which uses Julia's Serialization stdlib, and can also be `jld` to use JLD.jl.
- `BENCHMARK_RENDER`: Configures rendering, which is disabled by default. Can be "live" or "offline", which are explained below.
- `BENCHMARK`: Specifies the set of benchmarks to run as a comma-separated list, where each entry can be one of `cpu`, `cuda`, or `amdgpu`, and may optionally append `+dagger` (like `cuda+dagger`) to indicate whether or not to use Dagger. Defaults to `cpu,cpu+dagger`, which runs CPU benchmarks with and without Dagger.
- `BENCHMARK_SCALE`: Determines how much to scale the benchmark sizing by, typically specified as a `UnitRange{Int}`. Defaults to `1:5:50`, which runs each scale from 1 to 50, in steps of 5.

## Rendering with `BENCHMARK_RENDER`

Dagger contains visualization code for the scheduler (as a Gantt chart) and thunk execution profiling (flamechart), which can be enabled with `BENCHMARK_RENDER`. Additionally, rendering can be done "live", served via a Mux.jl webserver run locally, or "offline", where the visualization will be embedded into the results output file. By default, rendering is disabled. If `BENCHMARK_RENDER` is set to `live`, a Mux webserver is started at `localhost:8000` (the address is not yet configurable), and the Gantt chart and profiling flamechart will be rendered once the benchmarks start. If set to `offline`, data visualization will happen in the background, and will be passed in the results file.

Note that Gantt chart and flamechart output is only generated and relevant during Dagger execution.

## TODO: Plotting
