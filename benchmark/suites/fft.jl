# 3D pencil-decomposed FFT benchmark suite (DArray).
#
# Exercises `AbstractFFTsExt`'s 3D pencil transform (`ext/AbstractFFTsExt.jl`):
# under the hood this is five consecutive `spawn_datadeps` regions chained by
# plain `copyto!`s (region -> transpose -> region -> transpose -> region),
# each region doing one uniform pass of 1D FFTs over every tile along one
# axis. That shape -- many small regions, all-to-all movement between them --
# is exactly what locality-aware Datadeps placement (`DATADEPS_LOCALITY_BIAS`)
# targets, and what a region-barrier-free pipeline (later phases) is meant to
# overlap.
#
# `using FFTW` (rather than `AbstractFFTs` directly) matches `test/array/fft.jl`:
# it pulls in `AbstractFFTs.fft`/`fft!`/`ifft`/`ifft!`, and loading it is what
# triggers `AbstractFFTsExt` (declared to activate on `AbstractFFTs` in
# `Project.toml`) once `Dagger` is also loaded.
#
# Operands are allocated inside each benchmark's `setup` (and freed in
# `teardown`) so only the running size is resident; sizes whose estimated peak
# allocation exceeds the memory budget are skipped. A cube grows as N^3, so
# (unlike the 2D `linalg`/`array` suites) only the smaller entries of the
# default `scales` ladder tend to fit the memory budget -- this is expected,
# not a bug, and matches how every suite here adapts to the machine.

using FFTW

"""Estimated bytes for `nmats` dense N×N×N cubes of element type `T`."""
cube_bytes(N; nmats=1, T=ComplexF64) = nmats * Int(N)^3 * sizeof(T)

function fft_suite(ctx; method, accels)
    @assert method == "dagger" "FFT suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "FFT suite only supports CPU execution"

    T = ComplexF64
    suite = BenchmarkGroup()

    # Capability probe (run once, at a tiny size): a baseline revision in an
    # AirspeedVelocity comparison may lack 3D pencil FFT support (or FFT
    # support at all), in which case running the kernel would abort the whole
    # benchmark run.
    pencil_ok = supported("fft/pencil") do
        DA = rand(Blocks(2, 2, 2), T, 4, 4, 4)
        wait(DA)
        fft(DA; decomp=:pencil)
    end

    for N in scales
        # `square_block` targets a *square* tile of N elements per side; reused
        # here as the per-axis pencil width (grid size N, "pencil count" ==
        # cld(N, b) pencils per axis), exactly as the dense suites reuse it for
        # 2D tile size -- same knob (`BENCHMARK_BLOCKSIZE`), one more dimension.
        b = square_block(N)
        sub = BenchmarkGroup()

        if pencil_ok
            # In-place holds the input plus the internal pencil-transpose
            # temporaries (`_fft!`'s A/B/C, each full-size).
            if fits_budget(cube_bytes(N; nmats=4, T=T))
                sub["fft! (Pencil, in-place)"] = @benchmarkable(
                    fft!(DA; decomp=:pencil),
                    setup = (DA = rand(Blocks($b, $b, $b), $T, $N, $N, $N); wait(DA)),
                    teardown = (DA = nothing; @everywhere GC.gc()))

                sub["ifft! (Pencil, in-place)"] = @benchmarkable(
                    ifft!(DA; decomp=:pencil),
                    setup = (DA = rand(Blocks($b, $b, $b), $T, $N, $N, $N); wait(DA)),
                    teardown = (DA = nothing; @everywhere GC.gc()))
            end

            # Out-of-place additionally holds the output DArray alongside the
            # (still-resident) input.
            if fits_budget(cube_bytes(N; nmats=5, T=T))
                sub["fft (Pencil, out-of-place)"] = @benchmarkable(
                    fft(DA; decomp=:pencil),
                    setup = (DA = rand(Blocks($b, $b, $b), $T, $N, $N, $N); wait(DA)),
                    teardown = (DA = nothing; @everywhere GC.gc()))
            end
        end

        isempty(sub) || (suite["N=$N (block $b)"] = sub)
    end

    suite
end

fft_suite
