import DataAPI: leftjoin, innerjoin

const JOINKWARGS = Set([
        :l_sorted,
        :r_sorted,
        :r_unique,
        :build_lookup,
        :lookup_table,
    ])

# so the function selection goes as follows:
# xjoin(d1, d2; on) ->  default definition, will call the specialized
#                       dataframes definition or the generic one from here
# xjoin(d1, d2; on, <any of the JOINKWARGS>) -> will always call the
#                       generic definition from here even if you pass dataframes

function leftjoin(d1::DTable, d2; kwargs...)
    f = if any(k in JOINKWARGS for k in keys(kwargs))
        (l, r, ks) -> _leftjoin(l, r; ks...)
    else
        (l, r, ks) -> leftjoin(l, r; ks...)
    end
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

leftjoin(l, r; on=nothing) = _leftjoin(l, r; on=on)
_leftjoin(l, r; kwargs...) = _join(:leftjoin, l, r; kwargs...)


function innerjoin(d1::DTable, d2; kwargs...)
    f = if any(k in JOINKWARGS for k in keys(kwargs))
        (l, r, ks) -> _innerjoin(l, r; ks...)
    else
        (l, r, ks) -> innerjoin(l, r; ks...)
    end
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

innerjoin(l, r; on=nothing) = _innerjoin(l, r; on=on)
_innerjoin(l, r; kwargs...) = _join(:innerjoin, l, r; kwargs...)


function _join(
        type::Symbol,
        l, r;
        on=nothing,
        l_sorted=false,
        r_sorted=false,
        r_unique=false,
        build_lookup=false,
        lookup_table=nothing
    )

    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)

    inner_l, inner_r = if r_sorted && l_sorted
        match_inner_indices_lsorted_rsorted(l, r, cmp_l, cmp_r, r_unique) # loop through r once
    elseif r_unique
        match_inner_indices_runique(l, r, cmp_l, cmp_r) # break on first match
    elseif r_sorted
        match_inner_indices_rsorted(l, r, cmp_l, cmp_r) # break on last match
    else
        match_inner_indices(l, r, cmp_l, cmp_r)
    end
    outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l, inner_l)
    build_joined_table(type, names, l, r, inner_l, inner_r, outer_l, other_r)
end
