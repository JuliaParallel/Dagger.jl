import DataAPI: leftjoin, innerjoin

const JOINKWARGS = Set([
    :l_sorted,
    :r_sorted,
    :r_unique,
    :lookup,
])


"""
    leftjoin(d1::DTable, d2; on=nothing, l_sorted=false, r_sorted=false, r_unique=false, lookup=nothing)

Perform a left join of `d1` with any `Tables.jl` compatible table type.
Returns a `DTable` with the result.

If the underlying table type happens to have a `leftjoin` implementation
and none of the below `DTable` related kwargs will be provided the specialized function will be used.
A good example of that is calling `leftjoin` on a `DTable` with a `DataFrame` underlying type
and a `d2` of `DataFrame` type.

# Keyword arguments

- `on`: Column symbols to join on. Can be provided as a symbol or a pair of symbols in case the column names differ. For joins on multiple columns a vector of the previously mentioned can be provided.
- `l_sorted`: To indicate the left table is sorted - only useful if the `r_sorted` is set to `true` as well.
- `r_sorted`: To indicate the right table is sorted.
- `r_unique`: To indicate the right table only contains unique keys.
- `lookup`: To provide a dict-like structure that will allow for quicker matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function leftjoin(d1::DTable, d2; kwargs...)
    f = if any(k in JOINKWARGS for k in keys(kwargs))
        (l, r, ks) -> _leftjoin(l, r; ks...)
    else
        (l, r, ks) -> leftjoin(l, r; ks...)
    end
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function leftjoin(d1::GDTable, d2; kwargs...)
    d = leftjoin(d1.dtable, d2; kwargs...)
    GDTable(d, d1.cols, d1.index)
end

leftjoin(l, r; on=nothing) = _leftjoin(l, r; on=on)
_leftjoin(l, r; kwargs...) = _join(:leftjoin, l, r; kwargs...)


"""
    innerjoin(d1::DTable, d2; on=nothing, l_sorted=false, r_sorted=false, r_unique=false, lookup=nothing)

Perform an inner join of `d1` with any `Tables.jl` compatible table type.
Returns a `DTable` with the result.

If the underlying table type happens to have a `innerjoin` implementation
and none of the below `DTable` related kwargs will be provided the specialized function will be used.
A good example of that is calling `innerjoin` on a `DTable` with a `DataFrame` underlying type
and a `d2` of `DataFrame` type.

# Keyword arguments

- `on`: Column symbols to join on. Can be provided as a symbol or a pair of symbols in case the column names differ. For joins on multiple columns a vector of the previously mentioned can be provided.
- `l_sorted`: To indicate the left table is sorted - only useful if the `r_sorted` is set to `true` as well.
- `r_sorted`: To indicate the right table is sorted.
- `r_unique`: To indicate the right table only contains unique keys.
- `lookup`: You can pass a dict-like structure here that will allow for quicker matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function innerjoin(d1::DTable, d2; kwargs...)
    f = if any(k in JOINKWARGS for k in keys(kwargs))
        (l, r, ks) -> _innerjoin(l, r; ks...)
    else
        (l, r, ks) -> innerjoin(l, r; ks...)
    end
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function innerjoin(d1::GDTable, d2; kwargs...)
    d = innerjoin(d1.dtable, d2; kwargs...)
    GDTable(d, d1.cols, d1.index)
end

innerjoin(l, r; on=nothing) = _innerjoin(l, r; on=on)
_innerjoin(l, r; kwargs...) = _join(:innerjoin, l, r; kwargs...)


function pick_match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
    if lookup !== nothing
        match_inner_indices_lookup(l, lookup, cmp_l) # uses the `lookup` to find indices
    elseif r_sorted && l_sorted
        match_inner_indices_lsorted_rsorted(l, r, cmp_l, cmp_r, r_unique) # loop through r once
    elseif r_unique
        match_inner_indices_runique(l, r, cmp_l, cmp_r) # break on first match
    elseif r_sorted
        match_inner_indices_rsorted(l, r, cmp_l, cmp_r) # break on last match
    else
        match_inner_indices(l, r, cmp_l, cmp_r)
    end
end

# this one is for DTable with Any joins
function _join(
        type::Symbol,
        l,
        r;
        on=nothing,
        l_sorted=false,
        r_sorted=false,
        r_unique=false,
        lookup=nothing
    )

    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)

    inner_l, inner_r = pick_match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)

    outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l, inner_l)
    build_joined_table(type, names, l, r, inner_l, inner_r, outer_l, other_r)
end

# this one is for DTable with DTable joins
function _join(
        type::Symbol,
        l,
        r::DTable;
        on=nothing,
        l_sorted=false,
        r_sorted=false,
        r_unique=false,
        lookup=nothing
    )

    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)

    process_one_chunk = (type, l, r, cmp_l, cmp_r, other_r, lookup, r_sorted, l_sorted, r_unique) -> begin
        inner_l, inner_r = pick_match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
        if type == :innerjoin
            return (Dagger.tochunk(build_joined_table(type, names, l, r, inner_l, inner_r, Set{UInt}(), other_r)),)
        elseif type == :leftjoin
            outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l, inner_l)
            return outer_l, Dagger.tochunk(build_joined_table(type, names, l, r, inner_l, inner_r, Set{UInt}(), other_r))
        end
    end

    vs = [Dagger.@spawn process_one_chunk(type, l, chunk, cmp_l, cmp_r, other_r, lookup, r_sorted, l_sorted, r_unique) for chunk in r.chunks]

    to_merge = Vector{Chunk}()
    v = fetch.(vs)

    if type == :innerjoin
        outer_l = Set{UInt}()
        append!(to_merge, getindex.(v, 1))
    elseif type == :leftjoin
        outer_l = intersect(getindex.(v, 1)...)
        append!(to_merge, getindex.(v, 2))
        inner_l = inner_r = Vector{UInt}() # to create a chunk with the unmatched rows on the left
        outer = Dagger.tochunk(build_joined_table(type, names, l, r, inner_l, inner_r, outer_l, other_r))
        push!(to_merge, outer)
    end

    merge_chunks(Tables.materializer(l), to_merge)
end
