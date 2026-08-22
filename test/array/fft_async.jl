# Re-runs the FFT test suite with hierarchical planning disabled, so
# `_fft!`/`_ifft!`'s internal, unconditional `Dagger.with(Dagger.DATADEPS_SYNC
# => false)` (ext/AbstractFFTsExt.jl, 2D/3D only) actually takes effect
# instead of being coerced back to `sync=true` by the default
# `hierarchical=true` (see the N.B. in `Dagger.spawn_datadeps`).
#
# N.B. This deliberately does *not* also bind `DATADEPS_SYNC => false` here:
# that would make it ambient for *every* `spawn_datadeps`/`copyto!` call this
# file reaches, not just `_fft!`/`_ifft!`'s own -- and this file's 1D
# `AbstractFFTs.fft!(DA::DVector)` mixes a Dagger region (`copyto!(A, DA)`)
# with plain, non-Dagger code (`AbstractFFTs.fft!(A)`) immediately after it,
# relying on that `copyto!` being a synchronous barrier. Forcing it async
# here raced the plain FFT against its own input and produced wrong answers
# (`A ≈ collect(DA)` failures at fft.jl:19/:89) -- caught by actually running
# this file, not by inspection. `sync=false` must only ever be requested by
# code that itself stays inside Dagger's tracking for everything downstream,
# which is exactly what `_fft!`/`_ifft!`'s own internal `with` does and
# nothing outside them should have to.
#
# Every numerical check in fft.jl still holds under just `hierarchical=false`:
# `_fft!`/`_ifft!` still fully synchronize (a trailing `Dagger.synchronize()`)
# before ever handing `output` back to the caller -- only the *internal*
# pipelining across the five consecutive regions a 3D pencil FFT is made of
# (copy-in, dim-1, dim-2, dim-3, ...) changes; 1D (which never opts in) is
# bit-for-bit unaffected.
Dagger.with(Dagger.DATADEPS_HIERARCHICAL => false) do
    include(joinpath(@__DIR__, "fft.jl"))
end
