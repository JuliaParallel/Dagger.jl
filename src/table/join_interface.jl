import DataAPI: leftjoin, innerjoin

# A set of kwargs that can be provided by the user.
# Used for deciding whether to use the `DTables` join implementation directly
# or to attempt using an external join function.
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
- `lookup`: To provide a dict-like structure that will allow for direct matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function leftjoin(d1::DTable, d2; kwargs...)
    f = (l, r, ks) -> _leftjoinwrapper(l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function leftjoin(d1::DTable, d2::DTable; kwargs...)
    f = (l, r, ks) -> _join(:leftjoin, l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function leftjoin(d1::GDTable, d2; kwargs...)
    d = leftjoin(d1.dtable, d2; kwargs...)
    GDTable(d, d1.cols, d1.index)
end


function _leftjoinwrapper(l, r; kwargs...)
    if !any(k in JOINKWARGS for k in keys(kwargs)) && use_dataframe_join(typeof(l), typeof(r))
        leftjoin(l, r; kwargs...)
    else
        _join(:leftjoin, l, r; kwargs...)
    end
end


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
- `lookup`: To provide a dict-like structure that will allow for direct matching of inner rows. The structure needs to contain keys in form of a `Tuple` and values in form of type `Vector{UInt}` containing the related row indices.
"""
function innerjoin(d1::DTable, d2; kwargs...)
    f = (l, r, ks) -> _innerjoinwrapper(l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function innerjoin(d1::DTable, d2::DTable; kwargs...)
    f = (l, r, ks) -> _join(:innerjoin, l, r; ks...)
    v = [Dagger.@spawn f(c, d2, kwargs) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function innerjoin(d1::GDTable, d2; kwargs...)
    d = innerjoin(d1.dtable, d2; kwargs...)
    GDTable(d, d1.cols, d1.index)
end

function _innerjoinwrapper(l, r; kwargs...)
    if !any(k in JOINKWARGS for k in keys(kwargs)) && use_dataframe_join(typeof(l), typeof(r))
        innerjoin(l, r; kwargs...)
    else
        _join(:innerjoin, l, r; kwargs...)
    end
end


"""
    match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)

Function responsible for picking the optimal method of joining inner indices depending on the
additional information about the tables provided by the user.
"""
function match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
    if lookup !== nothing
        match_inner_indices_lookup(l, lookup, cmp_l) # uses the `lookup` to find indices
    elseif r_sorted && l_sorted
        match_inner_indices_lsorted_rsorted(l, r, cmp_l, cmp_r, r_unique) # loop through r once
    elseif r_unique
        match_inner_indices_runique(l, r, cmp_l, cmp_r) # break on first match
    elseif r_sorted
        match_inner_indices_rsorted(l, r, cmp_l, cmp_r) # break on last match
    else # generic fallback, no optimization
        match_inner_indices(l, r, cmp_l, cmp_r)
    end
end

"""
    _join(type::Symbol, l_chunk, r; kwargs...)

Low level join method for `DTable` joins using the generic implementation.
It joins an `l_chunk` with `r` assuming `r` is a continuous table.
"""
function _join(
        type::Symbol,
        l_chunk,
        r;
        on=nothing,
        l_sorted=false,
        r_sorted=false,
        r_unique=false,
        lookup=nothing
    )

    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l_chunk, r, on)

    inner_l, inner_r = match_inner_indices(l_chunk, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)

    outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l_chunk, inner_l)
    build_joined_table(type, names, l_chunk, r, inner_l, inner_r, outer_l, other_r)
end


"""
    _join(type::Symbol, l_chunk, r::DTable; kwargs...)

Low level join method for `DTable` joins using the generic implementation.
It joins an `l_chunk` with `r` assuming `r` is a `DTable`.
In this case the join is split into multiple joins of `l_chunk` with each chunk of `r` and a final merge operation.
"""
function _join(
        type::Symbol,
        l_chunk,
        r::DTable;
        on=nothing,
        l_sorted=false,
        r_sorted=false,
        r_unique=false,
        lookup=nothing
    )

    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l_chunk, r, on)

    process_one_chunk = (type, l, r, cmp_l, cmp_r, other_r, lookup, r_sorted, l_sorted, r_unique) -> begin
        inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r, lookup, r_sorted, l_sorted, r_unique)
        inner_chunk = Dagger.tochunk(build_joined_table(type, names, l, r, inner_l, inner_r, Set{UInt}(), other_r))
        outer_l = type == :innerjoin ? Set{UInt}() : find_outer_indices(l, inner_l)
        return inner_chunk, outer_l
    end

    vs = [Dagger.@spawn process_one_chunk(type, l_chunk, chunk, cmp_l, cmp_r, other_r, lookup, r_sorted, l_sorted, r_unique) for chunk in r.chunks]

    to_merge = Vector{Chunk}()
    sizehint!(to_merge, length(r.chunks))

    v = fetch.(vs)
    append!(to_merge, getindex.(v, 1))

    if type == :leftjoin
        outer_l = intersect(getindex.(v, 2)...)
        inner_l = inner_r = Vector{UInt}()
        outer = Dagger.tochunk(build_joined_table(type, names, l_chunk, r, inner_l, inner_r, outer_l, other_r))
        push!(to_merge, outer)
    end

    merge_chunks(Tables.materializer(l_chunk), to_merge)
end

"""
    use_dataframe_join(d1type, d2type)

Determines whether to use the DataAPI join function, which leads to usage of DataFrames join function if both types are `DataFrame`.
Remove this function and it's usage once a generic Tables.jl compatible join function becomes available.
Porting the Dagger join functions to TableOperations is an option to achieve that.
"""
function use_dataframe_join(d1type, d2type)
    :DataFrame == d1type.name.name == d2type.name.name
end
