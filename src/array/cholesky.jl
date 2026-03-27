LinearAlgebra.cholcopy(A::DArray{T,2}) where T = copy(A)
function potrf_checked!(uplo, A, info_arr)
    _A, info = move(task_processor(), LAPACK.potrf!)(uplo, A)
    if info != 0
        fill!(info_arr, info)
        throw(PosDefException(info))
    end
    return _A, info
end

# ---------------------------------------------------------------------------
# Algorithm selection via ScopedValue
# ---------------------------------------------------------------------------

"""
    CHOLESKY_ALGORITHM

`ScopedValue{Symbol}` controlling which blocked Cholesky variant `_chol!` uses
for `DArray`.  Accepted values:

- `:rl`    – right-looking (original, no pinning / lookahead)
- `:rl_la` – right-looking with **processor pinning** and **lookahead** spawn
             ordering (default)
- `:ll`    – left-looking with **processor pinning** (GEMM-based updates)
"""
const CHOLESKY_ALGORITHM = ScopedValue{Symbol}(:rl_la)

# ---------------------------------------------------------------------------
# Processor-grid helper
# ---------------------------------------------------------------------------

function _chunk_proc_grid(Ac)
    mt, nt = size(Ac)
    grid = Matrix{Processor}(undef, mt, nt)
    for j in 1:nt, i in 1:mt
        grid[i, j] = processor(fetch(Ac[i, j]; raw=true))
    end
    return grid
end

# ---------------------------------------------------------------------------
# Right-looking  (original, no pinning, no lookahead)  –  :rl
# ---------------------------------------------------------------------------

function _chol_rl_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            Dagger.@spawn potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
            for n in range(k+1, nt)
                Dagger.@spawn BLAS.trsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, n]))
            end
            for m in range(k+1, mt)
                if iscomplex
                    Dagger.@spawn BLAS.herk!(uplo, 'C', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                else
                    Dagger.@spawn BLAS.syrk!(uplo, 'T', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                end
                for n in range(m+1, nt)
                    Dagger.@spawn BLAS.gemm!(trans, 'N', mzone, In(Ac[k, m]), In(Ac[k, n]), zone, InOut(Ac[m, n]))
                end
            end
        end
    end
end

function _chol_rl_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            Dagger.@spawn potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
            for m in range(k+1, mt)
                Dagger.@spawn BLAS.trsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[m, k]))
            end
            for n in range(k+1, nt)
                if iscomplex
                    Dagger.@spawn BLAS.herk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                else
                    Dagger.@spawn BLAS.syrk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                end
                for m in range(n+1, mt)
                    Dagger.@spawn BLAS.gemm!('N', trans, mzone, In(Ac[m, k]), In(Ac[n, k]), zone, InOut(Ac[m, n]))
                end
            end
        end
    end
end

# ---------------------------------------------------------------------------
# Right-looking + processor pinning + lookahead  –  :rl_la
# ---------------------------------------------------------------------------

function _chol_rl_la_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    grid = _chunk_proc_grid(Ac)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            # Panel factorize
            Dagger.@spawn scope=ExactScope(grid[k, k]) potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))

            # -- Critical path: trsm for column k+1 first --
            if k < nt
                Dagger.@spawn scope=ExactScope(grid[k, k+1]) BLAS.trsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, k+1]))
            end
            # -- Critical path: syrk/herk for diagonal k+1 --
            if k < mt
                if iscomplex
                    Dagger.@spawn scope=ExactScope(grid[k+1, k+1]) BLAS.herk!(uplo, 'C', rmzone, In(Ac[k, k+1]), rzone, InOut(Ac[k+1, k+1]))
                else
                    Dagger.@spawn scope=ExactScope(grid[k+1, k+1]) BLAS.syrk!(uplo, 'T', rmzone, In(Ac[k, k+1]), rzone, InOut(Ac[k+1, k+1]))
                end
            end

            # Remaining trsm (skip k+1 already done)
            for n in range(k+2, nt)
                Dagger.@spawn scope=ExactScope(grid[k, n]) BLAS.trsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, n]))
            end
            # Remaining syrk/herk + gemm (skip diagonal k+1 already done)
            for m in range(k+1, mt)
                if m != k+1
                    if iscomplex
                        Dagger.@spawn scope=ExactScope(grid[m, m]) BLAS.herk!(uplo, 'C', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    else
                        Dagger.@spawn scope=ExactScope(grid[m, m]) BLAS.syrk!(uplo, 'T', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    end
                end
                for n in range(m+1, nt)
                    Dagger.@spawn scope=ExactScope(grid[m, n]) BLAS.gemm!(trans, 'N', mzone, In(Ac[k, m]), In(Ac[k, n]), zone, InOut(Ac[m, n]))
                end
            end
        end
    end
end

function _chol_rl_la_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    grid = _chunk_proc_grid(Ac)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            # Panel factorize
            Dagger.@spawn scope=ExactScope(grid[k, k]) potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))

            # -- Critical path: trsm for row k+1 first --
            if k < mt
                Dagger.@spawn scope=ExactScope(grid[k+1, k]) BLAS.trsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k+1, k]))
            end
            # -- Critical path: syrk/herk for diagonal k+1 --
            if k < nt
                if iscomplex
                    Dagger.@spawn scope=ExactScope(grid[k+1, k+1]) BLAS.herk!(uplo, 'N', rmzone, In(Ac[k+1, k]), rzone, InOut(Ac[k+1, k+1]))
                else
                    Dagger.@spawn scope=ExactScope(grid[k+1, k+1]) BLAS.syrk!(uplo, 'N', rmzone, In(Ac[k+1, k]), rzone, InOut(Ac[k+1, k+1]))
                end
            end

            # Remaining trsm (skip k+1 already done)
            for m in range(k+2, mt)
                Dagger.@spawn scope=ExactScope(grid[m, k]) BLAS.trsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[m, k]))
            end
            # Remaining syrk/herk + gemm (skip diagonal k+1 already done)
            for n in range(k+1, nt)
                if n != k+1
                    if iscomplex
                        Dagger.@spawn scope=ExactScope(grid[n, n]) BLAS.herk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                    else
                        Dagger.@spawn scope=ExactScope(grid[n, n]) BLAS.syrk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                    end
                end
                for m in range(n+1, mt)
                    Dagger.@spawn scope=ExactScope(grid[m, n]) BLAS.gemm!('N', trans, mzone, In(Ac[m, k]), In(Ac[n, k]), zone, InOut(Ac[m, n]))
                end
            end
        end
    end
end

# ---------------------------------------------------------------------------
# Left-looking + processor pinning  –  :ll
# ---------------------------------------------------------------------------

function _chol_ll_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    grid = _chunk_proc_grid(Ac)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            # Update diagonal Ac[k,k] from all previous rows
            for j in range(1, k-1)
                if iscomplex
                    Dagger.@spawn scope=ExactScope(grid[k, k]) BLAS.herk!(uplo, 'C', rmzone, In(Ac[j, k]), rzone, InOut(Ac[k, k]))
                else
                    Dagger.@spawn scope=ExactScope(grid[k, k]) BLAS.syrk!(uplo, 'T', rmzone, In(Ac[j, k]), rzone, InOut(Ac[k, k]))
                end
            end
            # Update off-diagonal row Ac[k,n] from all previous rows
            for n in range(k+1, nt)
                for j in range(1, k-1)
                    Dagger.@spawn scope=ExactScope(grid[k, n]) BLAS.gemm!(trans, 'N', mzone, In(Ac[j, k]), In(Ac[j, n]), zone, InOut(Ac[k, n]))
                end
            end
            # Panel factorize
            Dagger.@spawn scope=ExactScope(grid[k, k]) potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
            # Solve row
            for n in range(k+1, nt)
                Dagger.@spawn scope=ExactScope(grid[k, n]) BLAS.trsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, n]))
            end
        end
    end
end

function _chol_ll_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    grid = _chunk_proc_grid(Ac)
    Dagger.spawn_datadeps() do
        for k in range(1, mt)
            # Update diagonal Ac[k,k] from all previous columns
            for j in range(1, k-1)
                if iscomplex
                    Dagger.@spawn scope=ExactScope(grid[k, k]) BLAS.herk!(uplo, 'N', rmzone, In(Ac[k, j]), rzone, InOut(Ac[k, k]))
                else
                    Dagger.@spawn scope=ExactScope(grid[k, k]) BLAS.syrk!(uplo, 'N', rmzone, In(Ac[k, j]), rzone, InOut(Ac[k, k]))
                end
            end
            # Update off-diagonal column Ac[i,k] from all previous columns
            for i in range(k+1, mt)
                for j in range(1, k-1)
                    Dagger.@spawn scope=ExactScope(grid[i, k]) BLAS.gemm!('N', trans, mzone, In(Ac[i, j]), In(Ac[k, j]), zone, InOut(Ac[i, k]))
                end
            end
            # Panel factorize
            Dagger.@spawn scope=ExactScope(grid[k, k]) potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
            # Solve column
            for i in range(k+1, mt)
                Dagger.@spawn scope=ExactScope(grid[i, k]) BLAS.trsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[i, k]))
            end
        end
    end
end

# ---------------------------------------------------------------------------
# Dispatch entry points (called by LinearAlgebra._chol!)
# ---------------------------------------------------------------------------

function _chol_dispatch_upper!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    algo = CHOLESKY_ALGORITHM[]
    maybe_copy_buffered(A => Blocks(min(A.partitioning.blocksize...), min(A.partitioning.blocksize...))) do A
        Ac = A.chunks
        mt, nt = size(Ac)
        if algo === :rl
            _chol_rl_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        elseif algo === :rl_la
            _chol_rl_la_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        elseif algo === :ll
            _chol_ll_upper!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        else
            throw(ArgumentError("Unknown CHOLESKY_ALGORITHM: $algo. Use :rl, :rl_la, or :ll"))
        end
    end
end

function _chol_dispatch_lower!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    algo = CHOLESKY_ALGORITHM[]
    maybe_copy_buffered(A => Blocks(min(A.partitioning.blocksize...), min(A.partitioning.blocksize...))) do A
        Ac = A.chunks
        mt, nt = size(Ac)
        if algo === :rl
            _chol_rl_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        elseif algo === :rl_la
            _chol_rl_la_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        elseif algo === :ll
            _chol_ll_lower!(Ac, mt, nt, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        else
            throw(ArgumentError("Unknown CHOLESKY_ALGORITHM: $algo. Use :rl, :rl_la, or :ll"))
        end
    end
end

# ---------------------------------------------------------------------------
# LinearAlgebra entry points
# ---------------------------------------------------------------------------

function LinearAlgebra._chol!(A::DArray{T,2}, ::Type{UpperTriangular}) where T
    LinearAlgebra.checksquare(A)

    zone = one(T)
    mzone = -one(T)
    rzone = one(real(T))
    rmzone = -one(real(T))
    uplo = 'U'
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        _chol_dispatch_upper!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return UpperTriangular(A), info[1]
end

function LinearAlgebra._chol!(A::DArray{T,2}, ::Type{LowerTriangular}) where T
    LinearAlgebra.checksquare(A)

    zone = one(T)
    mzone = -one(T)
    rzone = one(real(T))
    rmzone = -one(real(T))
    uplo = 'L'
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        _chol_dispatch_lower!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return LowerTriangular(A), info[1]
end

"""
    darray_cholesky!(A::DArray{T,2}; uplo::Char='L', check::Bool=true) -> Cholesky

In-place blocked Cholesky on a `DArray`, bypassing `LinearAlgebra.cholesky!`
entirely (no `ishermitian` check, no `Hermitian` wrapper, no method dispatch
through `AbstractMatrix`).  Calls directly into the Dagger `_chol_dispatch_*`
kernel selected by `CHOLESKY_ALGORITHM[]`.

Returns a standard `LinearAlgebra.Cholesky` object whose `.factors` field is
the mutated `A`.
"""
function darray_cholesky!(A::DArray{T,2}; uplo::Char='L', check::Bool=true) where T
    LinearAlgebra.checksquare(A)

    zone  = one(T)
    mzone = -one(T)
    rzone  = one(real(T))
    rmzone = -one(real(T))
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        if uplo == 'U'
            _chol_dispatch_upper!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        else
            _chol_dispatch_lower!(A, info, zone, mzone, rzone, rmzone, uplo, iscomplex, trans)
        end
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    check && LinearAlgebra.checkpositivedefinite(info[1])
    return Cholesky(A, uplo, info[1])
end
