module EnzymeExt

import Dagger
import Dagger: DArray, DTask, indexes, poolget, fetch

import Enzyme
const EC = Enzyme.EnzymeCore

## make_zero for DArray shadows

function _chunk_shape(d::DArray, idx)
    return map(length, indexes(d.subdomains[idx]))
end

function _zero_darray_chunks(d::DArray{T}) where T
    chunks = similar(d.chunks)
    for idx in eachindex(d.chunks)
        chunks[idx] = zeros(T, _chunk_shape(d, idx)...)
    end
    return chunks
end

function _zero_darray(prev::DArray{T,N,B,F}) where {T,N,B,F}
    return DArray(T, prev.domain, prev.subdomains, _zero_darray_chunks(prev),
                  prev.partitioning, prev.concat)
end

function _materialize_chunks!(d::DArray)
    fetched = fetch(d)
    for idx in eachindex(d.chunks)
        d.chunks[idx] = _chunk_array(fetched.chunks[idx])
    end
    return d
end

function EC.make_zero(
    ::Type{DA}, seen::IdDict, prev::DA, ::Val{copy_if_inactive}
) where {T,N,B,F,DA<:DArray{T,N,B,F}, copy_if_inactive}
    if haskey(seen, prev)
        return seen[prev]
    end
    _materialize_chunks!(prev)
    new = _zero_darray(prev)::DA
    seen[prev] = new
    return new
end

function _zero_darray_chunks!(d::DArray{T}) where T
    for idx in eachindex(d.chunks)
        chunk = d.chunks[idx]
        chunk isa AbstractArray && fill!(chunk, zero(T))
    end
    return nothing
end

function EC.make_zero!(prev::DArray, seen)::Nothing
    if seen !== nothing
        prev in seen && return nothing
        push!(seen, prev)
    end
    _zero_darray_chunks!(prev)
    return nothing
end

EC.make_zero!(prev::DArray) = EC.make_zero!(prev, nothing)

function EC.remake_zero!(prev::DArray, seen)::Nothing
    return EC.make_zero!(prev, seen)
end

EC.remake_zero!(prev::DArray) = EC.remake_zero!(prev, nothing)

## Chunk materialization for custom rules

function _chunk_array(c)
    c isa Dagger.Chunk && return poolget(c.handle)
    c isa DTask && return _chunk_array(fetch(c))
    c isa AbstractArray && return c
    error("Unsupported DArray chunk type $(typeof(c)) for Enzyme AD")
end

function _materialize_chunk_arrays(x::DArray)
    fetched = fetch(x)
    return map(_chunk_array, fetched.chunks)
end

function _seeded_sum_gradient!(dA, A, f, seed)
    local_dA = zero(A)
    Enzyme.autodiff(Enzyme.Reverse, a -> seed * sum(f, a),
                    EC.Active, EC.Duplicated(A, local_dA))
    dA .+= local_dA
    return nothing
end

function _darray_sum_primal(config, RT, f, x)
    val = EC.ignore_derivatives(Base.sum(f.val, x.val))
    tape = (_materialize_chunk_arrays(x.val), f.val)
    primal = EC.EnzymeRules.needs_primal(config) ? val : nothing
    return EC.EnzymeRules.augmented_rule_return_type(config, RT)(primal, nothing, tape)
end

function _darray_sum_reverse(dret, tape, x)
    chunk_arrays, f_val = tape
    seed = dret.val
    for idx in eachindex(chunk_arrays)
        _seeded_sum_gradient!(x.dval.chunks[idx], chunk_arrays[idx], f_val, seed)
    end
    return (nothing, nothing, nothing, nothing)
end

const _mapreduce_maybesync = Dagger._mapreduce_maybesync

function _darray_mapreduce_primal(config, RT, f, op_inner, op_outer, x, dims, init)
    val = EC.ignore_derivatives(_mapreduce_maybesync(
        f.val, op_inner.val, op_outer.val, x.val, dims.val, init.val))
    chunk_arrays = _materialize_chunk_arrays(x.val)
    shadow_ref = RT <: EC.Duplicated ? Ref(zero(val)) : nothing
    tape = (chunk_arrays, f.val, op_inner.val, op_outer.val, shadow_ref)
    primal = EC.EnzymeRules.needs_primal(config) ? val : nothing
    shadow = EC.EnzymeRules.needs_shadow(config) ? shadow_ref : nothing
    return EC.EnzymeRules.augmented_rule_return_type(config, RT)(primal, shadow, tape)
end

function _darray_mapreduce_reverse(dret, tape, x)
    chunk_arrays, f_val, op_inner_val, op_outer_val, shadow_ref = tape
    seed = dret === nothing ? something(shadow_ref, Ref(1.0))[] : dret.val
    for idx in eachindex(chunk_arrays)
        if op_inner_val === Base.sum
            _seeded_sum_gradient!(x.dval.chunks[idx], chunk_arrays[idx], f_val, seed)
        else
            local_dA = zero(chunk_arrays[idx])
            Enzyme.autodiff(Enzyme.Reverse, a -> seed * mapreduce(f_val, op_outer_val, a),
                            EC.Active, EC.Duplicated(chunk_arrays[idx], local_dA))
            x.dval.chunks[idx] .+= local_dA
        end
    end
    return ntuple(Returns(nothing), 6)
end

function EC.EnzymeRules.augmented_primal(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(_mapreduce_maybesync)},
    RT::Type{<:EC.Active},
    f::EC.Const,
    op_inner::EC.Const,
    op_outer::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_mapreduce_primal(config, RT, f, op_inner, op_outer, x, dims, init)
end

function EC.EnzymeRules.augmented_primal(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(_mapreduce_maybesync)},
    RT::Type{<:EC.Duplicated},
    f::EC.Const,
    op_inner::EC.Const,
    op_outer::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_mapreduce_primal(config, RT, f, op_inner, op_outer, x, dims, init)
end

function EC.EnzymeRules.reverse(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(_mapreduce_maybesync)},
    dret::EC.Duplicated,
    tape,
    f::EC.Const,
    op_inner::EC.Const,
    op_outer::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_mapreduce_reverse(EC.Active(dret.val), tape, x)
end

function EC.EnzymeRules.reverse(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(_mapreduce_maybesync)},
    ::Type{<:EC.DuplicatedNoNeed},
    tape,
    f::EC.Const,
    op_inner::EC.Const,
    op_outer::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_mapreduce_reverse(nothing, tape, x)
end

function EC.EnzymeRules.reverse(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(_mapreduce_maybesync)},
    dret::EC.Active,
    tape,
    f::EC.Const,
    op_inner::EC.Const,
    op_outer::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_mapreduce_reverse(dret, tape, x)
end

## Scalar sum on DArray (e.g. sum(abs2, x))

function EC.EnzymeRules.augmented_primal(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(Base.sum)},
    RT::Type{<:EC.Active},
    f::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_sum_primal(config, RT, f, x)
end

function EC.EnzymeRules.reverse(
    config::EC.EnzymeRules.RevConfigWidth{1},
    func::EC.Const{typeof(Base.sum)},
    dret::EC.Active,
    tape,
    f::EC.Const,
    x::EC.Duplicated,
    dims::EC.Const{Nothing},
    init::EC.Const,
)
    return _darray_sum_reverse(dret, tape, x)
end

end # module EnzymeExt
