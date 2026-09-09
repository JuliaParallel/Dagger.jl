export @einsum

# Dagger.@einsum — tiled Einstein summation on DArrays.
#
# Analogous to `@stencil`: the macro owns the tile loop and the communication
# (one `spawn_datadeps` region, one task per output tile × contracted tile).
# It is not a wrapper around TensorOperations / OMEinsum / Tullio; those
# packages still need their own DArray tensor backend (a second invention).
#
#   @einsum C[i,j] = A[i,k] * B[k,j]     # in-place GEMM
#   @einsum C[i,j] := A[i,k] * B[k,j]    # allocate
#   @einsum A[i,k] * B[k,j]              # allocate and return
#   @einsum s = A[i] * B[i]              # tiled dot
#   @einsum C[i] = A[i,j] * x[j]         # GEMV
#
# Two-tensor products that LinearAlgebra already expresses stay available as
# `mul!` / `*` / `dot`; `@einsum` is the public entry for the notation.

#############################################################################
# Public macro
#############################################################################

"""
    @einsum expr
    @einsum begin expr... end

Einstein-summation notation for [`DArray`](@ref)s. Repeated indices are
contracted; a left-hand side names the free indices. The macro lowers to tiled
Datadeps tasks (the same communication model as [`@stencil`](@ref)), so
operands that already live in chunks stay there.

```julia
import Dagger: @einsum

A = rand(Blocks(4, 4), 8, 8)
B = rand(Blocks(4, 4), 8, 8)
C = zeros(Blocks(4, 4), 8, 8)

@einsum C[i,j] = A[i,k] * B[k,j]   # same values as `mul!(C, A, B)`
D = @einsum A[i,k] * B[k,j]        # allocate
s = @einsum A[i,j] * B[i,j]        # Frobenius inner product
```

`:=` allocates a new `DArray` (or a scalar when every index is contracted).
`+=` accumulates into an existing destination. Host `Array` operands are
wrapped as a single tile. Index block sizes that share a name must match;
repartition first if they do not.

2-tensor products remain available through LinearAlgebra (`mul!`, `*`, `dot`);
`@einsum` is the notation, not a replacement for those methods.
"""
macro einsum(orig_ex)
    if !Meta.isexpr(orig_ex, :block)
        orig_ex = Expr(:block, orig_ex)
    end
    final_ex = Expr(:block)
    last_result = nothing
    for inner_ex in orig_ex.args
        inner_ex isa LineNumberNode && continue
        lowered, result = _einsum_lower_stmt(inner_ex)
        push!(final_ex.args, lowered)
        last_result = result
    end
    if last_result !== nothing
        push!(final_ex.args, last_result)
    end
    return esc(final_ex)
end

function _einsum_lower_stmt(ex)
    accumulate = false
    allocate = false
    if ex isa Expr && ex.head in (:(+=), :(-=))
        op = ex.head === :(+=) ? :+ : :-
        accumulate = true
        dest_ex, rhs = ex.args[1], ex.args[2]
        if op === :-
            rhs = Expr(:call, :*, -1, rhs)
        end
    elseif Meta.isexpr(ex, :(:=))
        allocate = true
        dest_ex, rhs = ex.args[1], ex.args[2]
    elseif Meta.isexpr(ex, :(=))
        dest_ex, rhs = ex.args[1], ex.args[2]
    else
        allocate = true
        dest_ex = nothing
        rhs = ex
    end

    dest, dest_inds = _einsum_parse_dest(dest_ex)
    terms, scalar_exs = _einsum_parse_product(rhs)
    isempty(terms) && throw(ArgumentError("@einsum: no indexed array in $ex"))

    arr_exs = Any[t[1] for t in terms]
    in_inds = Any[Expr(:tuple, QuoteNode.(t[2])...) for t in terms]
    α_ex = isempty(scalar_exs) ? 1 : foldl((a, b) -> Expr(:call, :*, a, b), scalar_exs)

    arrays_ex = Expr(:tuple, arr_exs...)
    in_inds_ex = Expr(:tuple, in_inds...)

    if dest_inds === nothing && !allocate
        # `s = A[i]*B[i]` — bare symbol destination, scalar reduction
        dest_inds_ex = :(())
        dest_arg = :(:scalar)
        result = dest
        lowered = :($dest = $_einsum_apply!($dest_arg, $dest_inds_ex, $arrays_ex, $in_inds_ex, $α_ex, $accumulate))
        return lowered, result
    end

    if dest_inds === nothing
        dest_inds_ex = :($(_einsum_infer_free)($in_inds_ex))
    else
        dest_inds_ex = Expr(:tuple, QuoteNode.(dest_inds)...)
    end

    if allocate || dest === nothing
        result = dest === nothing ? gensym("einsum_out") : dest
        dest_arg = nothing
        lowered = :($result = $_einsum_apply!($dest_arg, $dest_inds_ex, $arrays_ex, $in_inds_ex, $α_ex, $accumulate))
        return lowered, result
    end

    lowered = :($_einsum_apply!($dest, $dest_inds_ex, $arrays_ex, $in_inds_ex, $α_ex, $accumulate))
    return lowered, dest
end

function _einsum_parse_dest(dest_ex)
    dest_ex === nothing && return nothing, nothing
    if @capture(dest_ex, C_[inds__])
        all(i -> i isa Symbol, inds) || throw(ArgumentError("@einsum: destination indices must be symbols, got $dest_ex"))
        return C, Symbol[inds...]
    elseif dest_ex isa Symbol
        return dest_ex, nothing
    else
        throw(ArgumentError("@einsum: destination must be `C[i,j]` or a scalar name, got $dest_ex"))
    end
end

function _einsum_parse_product(rhs)
    terms = Tuple{Any,Vector{Symbol}}[]
    scalars = Any[]
    _einsum_walk_product!(rhs, terms, scalars)
    return terms, scalars
end

function _einsum_walk_product!(ex, terms, scalars)
    if @capture(ex, A_ * B_)
        _einsum_walk_product!(A, terms, scalars)
        _einsum_walk_product!(B, terms, scalars)
    elseif @capture(ex, A_[inds__])
        all(i -> i isa Symbol, inds) || throw(ArgumentError("@einsum: indices must be symbols, got $ex"))
        push!(terms, (A, Symbol[inds...]))
    else
        push!(scalars, ex)
    end
    return
end

function _einsum_infer_free(in_inds)
    counts = Dict{Symbol,Int}()
    for inds in in_inds
        for s in inds
            counts[s] = get(counts, s, 0) + 1
        end
    end
    free = Symbol[]
    seen = Set{Symbol}()
    for inds in in_inds
        for s in inds
            if counts[s] == 1 && !(s in seen)
                push!(free, s)
                push!(seen, s)
            end
        end
    end
    return Tuple(free)
end

#############################################################################
# Runtime
#############################################################################

function _einsum_promote_arrays(arrays, in_inds)
    sizes = Dict{Symbol,Int}()
    blocks = Dict{Symbol,Int}()
    for (A, inds) in zip(arrays, in_inds)
        if A isa DArray
            A.partitioning isa Blocks || throw(ArgumentError("@einsum: operands must use `Blocks` partitioning"))
            ndims(A) == length(inds) || throw(ArgumentError("@einsum: $(ndims(A))-D array indexed with $(length(inds)) indices"))
            for (d, s) in enumerate(inds)
                sz = size(A, d)
                bl = A.partitioning.blocksize[d]
                if haskey(sizes, s)
                    sizes[s] == sz || throw(DimensionMismatch("@einsum: index $s has sizes $(sizes[s]) and $sz"))
                    blocks[s] == bl || throw(DimensionMismatch(
                        "@einsum: index $s has block sizes $(blocks[s]) and $bl; repartition so they match"))
                else
                    sizes[s] = sz
                    blocks[s] = bl
                end
            end
        elseif A isa AbstractArray
            ndims(A) == length(inds) || throw(ArgumentError("@einsum: $(ndims(A))-D array indexed with $(length(inds)) indices"))
            for (d, s) in enumerate(inds)
                sz = size(A, d)
                if haskey(sizes, s)
                    sizes[s] == sz || throw(DimensionMismatch("@einsum: index $s has sizes $(sizes[s]) and $sz"))
                else
                    sizes[s] = sz
                end
            end
        end
    end
    out = Any[]
    for (A, inds) in zip(arrays, in_inds)
        if A isa DArray
            push!(out, fetch(A))
        elseif A isa AbstractArray
            bl = ntuple(d -> get(blocks, inds[d], size(A, d)), ndims(A))
            push!(out, fetch(distribute(A, Blocks(bl...))))
        else
            throw(ArgumentError("@einsum: operand must be a DArray or AbstractArray, got $(typeof(A))"))
        end
    end
    return out
end

function _einsum_index_meta(arrays, inds_list)
    sizes = Dict{Symbol,Int}()
    blocks = Dict{Symbol,Int}()
    for (A, inds) in zip(arrays, inds_list)
        A isa DArray || continue
        ndims(A) == length(inds) || throw(ArgumentError("@einsum: $(ndims(A))-D array indexed with $(length(inds)) indices"))
        A.partitioning isa Blocks || throw(ArgumentError("@einsum: operands must use `Blocks` partitioning"))
        for (d, s) in enumerate(inds)
            sz = size(A, d)
            bl = A.partitioning.blocksize[d]
            if haskey(sizes, s)
                sizes[s] == sz || throw(DimensionMismatch("@einsum: index $s has sizes $(sizes[s]) and $sz"))
                blocks[s] == bl || throw(DimensionMismatch(
                    "@einsum: index $s has block sizes $(blocks[s]) and $bl; repartition so they match"))
            else
                sizes[s] = sz
                blocks[s] = bl
            end
        end
    end
    return sizes, blocks
end

function _einsum_tile_ci(inds, coords)
    return CartesianIndex(ntuple(d -> coords[inds[d]], length(inds)))
end

function _einsum_allocate(arrays, in_inds, out_inds)
    sizes, blocks = _einsum_index_meta(arrays, in_inds)
    isempty(out_inds) && return nothing
    dims = ntuple(d -> sizes[out_inds[d]], length(out_inds))
    part = Blocks(ntuple(d -> blocks[out_inds[d]], length(out_inds)))
    T = promote_type(map(eltype, arrays)...)
    darrs = filter(A -> A isa DArray, arrays)
    if length(out_inds) == 2 && !isempty(darrs) && all(is_sparse_backed, darrs)
        return allocate_tiled(DSparseArray{T,2}, T, part, dims)
    end
    return zeros(part, T, dims...)
end

"""
    _einsum_apply!(dest, dest_inds, arrays, in_inds, α, accumulate)

Runtime for [`@einsum`](@ref). `dest` is a `DArray`, `nothing` (allocate), or
`:scalar`. `dest_inds` / `in_inds` are tuples of index symbols.
"""
function _einsum_apply!(dest, dest_inds, arrays, in_inds, α, accumulate)
    darrs = _einsum_promote_arrays(arrays, in_inds)
    if dest === :scalar || (dest === nothing && isempty(dest_inds))
        return _einsum_reduce(darrs, in_inds, α)
    end
    C = dest === nothing ? _einsum_allocate(darrs, in_inds, dest_inds) : dest
    C isa DArray || throw(ArgumentError("@einsum: destination must be a DArray, got $(typeof(C))"))
    _einsum_contract!(C, darrs, in_inds, dest_inds, α, accumulate)
    return dest === nothing ? fetch(C) : dest
end

function _einsum_contract!(C::DArray, arrays, in_inds, out_inds, α, accumulate)
    sizes, blocks = _einsum_index_meta((C, arrays...), (out_inds, in_inds...))
    ntiles = Dict{Symbol,Int}(s => Int(cld(sizes[s], blocks[s])) for s in keys(sizes))
    contracted = Symbol[s for s in keys(sizes) if !(s in out_inds)]
    sort!(contracted)  # stable tile walk
    out_shape = ntuple(d -> ntiles[out_inds[d]], length(out_inds))
    C = fetch(C)
    arrays = map(A -> A isa DArray ? fetch(A) : A, arrays)

    Dagger.spawn_datadeps() do
        for out_I in CartesianIndices(out_shape)
            free_coord = Dict{Symbol,Int}(out_inds[d] => out_I[d] for d in 1:length(out_inds))
            if isempty(contracted)
                _einsum_spawn_tile!(C, arrays, in_inds, out_inds, free_coord, Dict{Symbol,Int}(), α,
                                    accumulate ? 1 : 0)
            else
                kshape = ntuple(d -> ntiles[contracted[d]], length(contracted))
                firstk = true
                for kI in CartesianIndices(kshape)
                    kcoord = Dict{Symbol,Int}(contracted[d] => kI[d] for d in 1:length(contracted))
                    β = (accumulate || !firstk) ? 1 : 0
                    _einsum_spawn_tile!(C, arrays, in_inds, out_inds, free_coord, kcoord, α, β)
                    firstk = false
                end
            end
        end
    end
    return C
end

function _einsum_spawn_tile!(C, arrays, in_inds, out_inds, free_coord, kcoord, α, β)
    coords = merge(free_coord, kcoord)
    Ctile = C.chunks[_einsum_tile_ci(out_inds, coords)]
    n = length(arrays)
    if n == 1
        Dagger.@spawn name="einsum_tile" _einsum_tile1!(InOut(Ctile), In(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)]),
                                                        out_inds, in_inds[1], α, β)
    elseif n == 2
        Dagger.@spawn name="einsum_tile" _einsum_tile2!(InOut(Ctile),
                                                        In(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)]),
                                                        In(arrays[2].chunks[_einsum_tile_ci(in_inds[2], coords)]),
                                                        out_inds, in_inds[1], in_inds[2], α, β)
    elseif n == 3
        Dagger.@spawn name="einsum_tile" _einsum_tile3!(InOut(Ctile),
                                                        In(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)]),
                                                        In(arrays[2].chunks[_einsum_tile_ci(in_inds[2], coords)]),
                                                        In(arrays[3].chunks[_einsum_tile_ci(in_inds[3], coords)]),
                                                        out_inds, in_inds[1], in_inds[2], in_inds[3], α, β)
    else
        throw(ArgumentError("@einsum: at most 3 tensor factors are supported (got $n); split the product"))
    end
    return
end

function _einsum_reduce(arrays, in_inds, α)
    sizes, blocks = _einsum_index_meta(arrays, in_inds)
    all_inds = Symbol[]
    seen = Set{Symbol}()
    for inds in in_inds
        for s in inds
            if !(s in seen)
                push!(all_inds, s)
                push!(seen, s)
            end
        end
    end
    T = promote_type(map(eltype, arrays)...)
    R = typeof(α * zero(T))
    isempty(all_inds) && return R(α)
    ntiles = Dict{Symbol,Int}(s => Int(cld(sizes[s], blocks[s])) for s in all_inds)
    shape = ntuple(d -> ntiles[all_inds[d]], length(all_inds))
    arrays = map(A -> A isa DArray ? fetch(A) : A, arrays)
    parts = DTask[]
    for I in CartesianIndices(shape)
        coords = Dict{Symbol,Int}(all_inds[d] => I[d] for d in 1:length(all_inds))
        if length(arrays) == 1
            push!(parts, Dagger.@spawn _einsum_scalar1(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)],
                                                       in_inds[1], α))
        elseif length(arrays) == 2
            push!(parts, Dagger.@spawn _einsum_scalar2(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)],
                                                       arrays[2].chunks[_einsum_tile_ci(in_inds[2], coords)],
                                                       in_inds[1], in_inds[2], α))
        elseif length(arrays) == 3
            push!(parts, Dagger.@spawn _einsum_scalar3(arrays[1].chunks[_einsum_tile_ci(in_inds[1], coords)],
                                                       arrays[2].chunks[_einsum_tile_ci(in_inds[2], coords)],
                                                       arrays[3].chunks[_einsum_tile_ci(in_inds[3], coords)],
                                                       in_inds[1], in_inds[2], in_inds[3], α))
        else
            throw(ArgumentError("@einsum: at most 3 tensor factors are supported"))
        end
    end
    return sum(fetch, parts; init=zero(R))
end

#############################################################################
# Tile kernels (named: MPI-stable)
#############################################################################

_unwrap_einsum_tile(x::DSparseArray) = x.mat
_unwrap_einsum_tile(x) = x

function _einsum_tile1!(C, A, Cinds, Ainds, α, β)
    _einsum_generic_tile!(_unwrap_einsum_tile(C), (_unwrap_einsum_tile(A),), Cinds, (Ainds,), α, β)
    return C
end

function _einsum_tile2!(C, A, B, Cinds, Ainds, Binds, α, β)
    Cu, Au, Bu = _unwrap_einsum_tile(C), _unwrap_einsum_tile(A), _unwrap_einsum_tile(B)
    if _einsum_try_mul!(C, Cu, Au, Bu, Cinds, Ainds, Binds, α, β)
        return C
    end
    _einsum_generic_tile!(Cu, (Au, Bu), Cinds, (Ainds, Binds), α, β)
    return C
end

function _einsum_tile3!(C, A, B, D, Cinds, Ainds, Binds, Dinds, α, β)
    _einsum_generic_tile!(_unwrap_einsum_tile(C),
                          (_unwrap_einsum_tile(A), _unwrap_einsum_tile(B), _unwrap_einsum_tile(D)),
                          Cinds, (Ainds, Binds, Dinds), α, β)
    return C
end

function _einsum_scalar1(A, Ainds, α)
    return _einsum_generic_scalar((_unwrap_einsum_tile(A),), (Ainds,), α)
end
function _einsum_scalar2(A, B, Ainds, Binds, α)
    Au, Bu = _unwrap_einsum_tile(A), _unwrap_einsum_tile(B)
    if Ainds == Binds && length(Ainds) >= 1
        return α * LinearAlgebra.dot(vec(Au), vec(Bu))
    end
    return _einsum_generic_scalar((Au, Bu), (Ainds, Binds), α)
end
function _einsum_scalar3(A, B, D, Ainds, Binds, Dinds, α)
    return _einsum_generic_scalar((_unwrap_einsum_tile(A), _unwrap_einsum_tile(B), _unwrap_einsum_tile(D)),
                                  (Ainds, Binds, Dinds), α)
end

# LinearAlgebra fast paths for the common 2-tensor products. Einsum does not
# conjugate unless the user writes `conj`; use `transpose`, not `adjoint`.
function _einsum_try_mul!(Cwrap, C, A, B, Cinds, Ainds, Binds, α, β)
    if length(Cinds) == 2 && length(Ainds) == 2 && length(Binds) == 2
        i, j = Cinds
        kA = Ainds[1] == i ? Ainds[2] : (Ainds[2] == i ? Ainds[1] : nothing)
        kA === nothing && return false
        kB = Binds[1] == j ? Binds[2] : (Binds[2] == j ? Binds[1] : nothing)
        kB === nothing && return false
        kA == kB || return false
        k = kA
        tA = Ainds == (i, k) ? 'N' : Ainds == (k, i) ? 'T' : return false
        tB = Binds == (k, j) ? 'N' : Binds == (j, k) ? 'T' : return false
        return _einsum_muladd!(Cwrap, C, A, B, tA, tB, α, β)
    elseif length(Cinds) == 1 && length(Ainds) == 2 && length(Binds) == 1
        i = Cinds[1]
        j = Binds[1]
        if Ainds == (i, j)
            LinearAlgebra.mul!(C, A, B, α, β)
            return true
        elseif Ainds == (j, i)
            LinearAlgebra.mul!(C, transpose(A), B, α, β)
            return true
        end
    elseif length(Cinds) == 1 && length(Binds) == 2 && length(Ainds) == 1
        return _einsum_try_mul!(Cwrap, C, B, A, Cinds, Binds, Ainds, α, β)
    elseif length(Cinds) == 2 && length(Ainds) == 1 && length(Binds) == 1
        i, j = Cinds
        if Ainds == (i,) && Binds == (j,)
            LinearAlgebra.mul!(C, reshape(A, :, 1), transpose(reshape(B, :, 1)), α, β)
            return true
        elseif Ainds == (j,) && Binds == (i,)
            LinearAlgebra.mul!(C, reshape(B, :, 1), transpose(reshape(A, :, 1)), α, β)
            return true
        end
    end
    return false
end

function _einsum_muladd!(Cwrap, C, A, B, tA, tB, α, β)
    opA = tA == 'T' ? transpose(A) : A
    opB = tB == 'T' ? transpose(B) : B
    if Cwrap isa DSparseArray
        AB = opA * opB
        if iszero(β)
            Cwrap.mat = isone(α) ? AB : α * AB
        else
            Cwrap.mat = α * AB + β * Cwrap.mat
        end
        return true
    end
    if hasmethod(LinearAlgebra.mul!, typeof((C, opA, opB, α, β)))
        LinearAlgebra.mul!(C, opA, opB, α, β)
    else
        AB = opA * opB
        if iszero(β)
            copyto!(C, isone(α) ? AB : α * AB)
        else
            C .= α .* AB .+ β .* C
        end
    end
    return true
end

function _einsum_generic_tile!(C, tensors, Cinds, Tinds, α, β)
    sizes = Dict{Symbol,Int}()
    function absorb!(inds, arr)
        for (d, s) in enumerate(inds)
            sz = size(arr, d)
            if haskey(sizes, s)
                sizes[s] == sz || throw(DimensionMismatch("@einsum: index $s has sizes $(sizes[s]) and $sz"))
            else
                sizes[s] = sz
            end
        end
    end
    absorb!(Cinds, C)
    for (t, inds) in zip(tensors, Tinds)
        absorb!(inds, t)
    end
    if iszero(β)
        fill!(C, zero(eltype(C)))
    elseif !isone(β)
        C .*= β
    end
    all_inds = collect(keys(sizes))
    isempty(all_inds) && return C
    ranges = ntuple(i -> 1:sizes[all_inds[i]], length(all_inds))
    NC = length(Cinds)
    for I in CartesianIndices(ranges)
        coord = ntuple(d -> I[d], length(all_inds))
        lookup = Dict{Symbol,Int}(all_inds[d] => coord[d] for d in 1:length(all_inds))
        prod = α
        for (t, inds) in zip(tensors, Tinds)
            prod *= t[ntuple(d -> lookup[inds[d]], length(inds))...]
        end
        if NC == 0
            continue
        end
        C[ntuple(d -> lookup[Cinds[d]], NC)...] += prod
    end
    return C
end

function _einsum_generic_scalar(tensors, Tinds, α)
    sizes = Dict{Symbol,Int}()
    for (t, inds) in zip(tensors, Tinds)
        for (d, s) in enumerate(inds)
            sz = size(t, d)
            if haskey(sizes, s)
                sizes[s] == sz || throw(DimensionMismatch("@einsum: index $s"))
            else
                sizes[s] = sz
            end
        end
    end
    T = typeof(α * zero(eltype(first(tensors))))
    s = zero(T)
    all_inds = collect(keys(sizes))
    isempty(all_inds) && return T(α)
    ranges = ntuple(i -> 1:sizes[all_inds[i]], length(all_inds))
    for I in CartesianIndices(ranges)
        lookup = Dict{Symbol,Int}(all_inds[d] => I[d] for d in 1:length(all_inds))
        prod = α
        for (t, inds) in zip(tensors, Tinds)
            prod *= t[ntuple(d -> lookup[inds[d]], length(inds))...]
        end
        s += prod
    end
    return s
end
