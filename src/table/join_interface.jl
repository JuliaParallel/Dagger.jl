import DataAPI: leftjoin, innerjoin

const JOINKWARGS = Set([
    :l_sorted,
    :r_sorted,
    :r_unique,
    :build_lookup,
    :lookup_table,])


function leftjoin(d1::DTable, d2; kwargs...)
    f = (_d1, _d2, kwargs) -> leftjoin(_d1, _d2; kwargs...)
    if any(k in JOINKWARGS for k in keys(kwargs))
        f = (_d1, _d2, kwargs) -> _leftjoin(_d1, _d2; kwargs...)
    end

    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

leftjoin(l, r; on = nothing) = _leftjoin(l, r; on = on)
_leftjoin(l, r; kwargs...) = _join(:leftjoin, l, r; kwargs...)


function innerjoin(d1::DTable, d2; kwargs...)
    f = (_d1, _d2, kwargs) -> innerjoin(_d1, _d2; kwargs...)
    if any(k in JOINKWARGS for k in keys(kwargs))
        f = (_d1, _d2, kwargs) -> _innerjoin(_d1, _d2; kwargs...)
    end
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

innerjoin(l, r; on = nothing) = _innerjoin(l, r; on = on)
_innerjoin(l, r; kwargs...) = _join(:innerjoin, l, r; kwargs...)


function _join(
        type::Symbol,
        l, r;
        on = nothing,
        l_sorted = false,
        r_sorted = false,
        r_unique = false,
        build_lookup = false,
        lookup_table = nothing
    )
    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)
    inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r)
    outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l, inner_l)
    build_joined_table(type, names, l, r, inner_l, inner_r, outer_l, other_r)
end
