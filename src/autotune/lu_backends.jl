# lu_backends.jl
#
# External dense-LU backends for the `:lu`/`:lu!` operations: MKL, OpenBLAS,
# BLIS (via LAPACK), and RecursiveFactorization. These let the autotuner pick
# the fastest LU implementation available on the machine (Dagger is not
# privileged) -- e.g. `DaggerLUFactorization` in LinearSolve routes through
# `:lu`, so it transparently gains whichever backend wins here.
#
# The BLAS/LAPACK backends are invoked exactly the way LinearSolve does it
# (see `LinearSolve/src/mkl.jl`, `.../openblas.jl`, and the BLIS extension):
# a direct `getrf!` into the vendor library, bypassing libblastrampoline, then
# a standard `LinearAlgebra.LU` is returned so the ordinary `ldiv!`/getrs path
# handles the (cheap) triangular solves. Everything is reached lazily through
# `Base.loaded_modules`, so Autotune keeps its stdlib-only dependency surface.

import Libdl

const _BlasInt = LinearAlgebra.BLAS.BlasInt

_lu_blas_prefix(::Type{Float64})    = "d"
_lu_blas_prefix(::Type{Float32})    = "s"
_lu_blas_prefix(::Type{ComplexF64}) = "z"
_lu_blas_prefix(::Type{ComplexF32}) = "c"

# `getrf` symbol for the active BLAS integer interface. Mirrors
# `LinearAlgebra.BLAS.@blasfunc`, which appends `64_` for the ILP64 build the
# Julia-shipped `*_jll` libraries use.
_getrf_symbol(::Type{T}) where {T} =
    Base.USE_BLAS64 ? Symbol(_lu_blas_prefix(T), "getrf_64_") :
                      Symbol(_lu_blas_prefix(T), "getrf_")

# dlsym is cheap but not free; cache the resolved function pointer per
# (library, eltype).
const _GETRF_PTRS = Dict{Tuple{String,DataType},Ptr{Cvoid}}()
const _GETRF_LOCK = ReentrantLock()
function _getrf_ptr(libpath::AbstractString, ::Type{T}) where {T}
    key = (String(libpath), T)
    lock(_GETRF_LOCK) do
        get!(_GETRF_PTRS, key) do
            Libdl.dlsym(Libdl.dlopen(libpath), _getrf_symbol(T))
        end
    end
end

# In-place LU (`A` is overwritten with the factors) via an external `getrf`,
# packaged as a `LinearAlgebra.LU` just like `lu!` would return.
function _external_getrf_lu!(libpath::AbstractString,
                             A::AbstractMatrix{T}) where {T<:LinearAlgebra.BlasFloat}
    Base.require_one_based_indexing(A)
    m, n = size(A)
    lda = max(1, stride(A, 2))
    ipiv = Vector{_BlasInt}(undef, min(m, n))
    info = Ref{_BlasInt}(0)
    ptr = _getrf_ptr(libpath, T)
    ccall(ptr, Cvoid,
          (Ref{_BlasInt}, Ref{_BlasInt}, Ptr{T}, Ref{_BlasInt}, Ptr{_BlasInt}, Ptr{_BlasInt}),
          m, n, A, lda, ipiv, info)
    return LinearAlgebra.LU(A, ipiv, Int(info[]))
end

# ---------------------------------------------------------------------------
# Lazy library resolution

# Load a package by name, falling back to a UUID lookup so transitive-only
# dependencies (which `Base.require(Main, sym)` can't reach) still resolve in
# benchmark worker processes.
function _ensure_loaded(name::AbstractString, uuid::AbstractString)
    m = loaded_module(name)
    m === nothing || return m
    return try
        Base.require(Base.PkgId(Base.UUID(uuid), String(name)))
    catch
        nothing
    end
end

function _mkl_getrf_lib()
    m = loaded_module("MKL_jll")
    m === nothing && return nothing
    return m.libmkl_rt
end

function _openblas_getrf_lib()
    m = loaded_module("OpenBLAS_jll")
    m === nothing && return nothing
    return m.libopenblas
end

# BLIS' LU (per LinearSolve's BLIS extension) actually factors through
# LAPACK_jll's `liblapack`; blis_jll supplies the BLAS backend. Ensure both.
const _LAPACK_JLL_UUID = "51474c39-65e3-53ba-86ba-03b1b862ec14"
function _blis_getrf_lib()
    loaded_module("blis_jll") === nothing && return nothing
    lp = _ensure_loaded("LAPACK_jll", _LAPACK_JLL_UUID)
    lp === nothing && return nothing
    return lp.liblapack
end

# Wrap a library getter into an `invoke`, erroring clearly if the library
# vanished between availability check and call.
function _external_lu_invoke(libgetter::Function, A)
    lib = libgetter()
    lib === nothing && error("Autotune: external LU backend library unavailable")
    return _external_getrf_lu!(lib, A)
end
