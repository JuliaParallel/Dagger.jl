import InvertedIndices: BroadcastedInvertedIndex
import DataAPI: Between, All, Cols, BroadcastedSelector
import DataFrames: ColumnIndex, MultiColumnIndex,
    ByRow, AsTable, normalize_selection

make_pair_concrete(@nospecialize(x::Pair)) =
    make_pair_concrete(x.first) => make_pair_concrete(x.second)
make_pair_concrete(@nospecialize(x)) = x

broadcast_pair(df::DTable, @nospecialize(p::Any)) = p

# Copied as is from DataFrames.jl
function broadcast_pair(df::DTable, @nospecialize(p::Pair))
    src, second = p
    src_broadcast = src isa Union{BroadcastedInvertedIndex,
        BroadcastedSelector}
    second_broadcast = second isa Union{BroadcastedInvertedIndex,
        BroadcastedSelector}
    if second isa Pair
        fun, dst = second
        dst_broadcast = dst isa Union{BroadcastedInvertedIndex,
            BroadcastedSelector}
        if src_broadcast || dst_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_dst = dst_broadcast ? names(df, dst.sel) : dst
            new_p = new_src .=> fun .=> new_dst
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    else
        if src_broadcast || second_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_second = second_broadcast ? names(df, second.sel) : second
            new_p = new_src .=> new_second
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    end
end

# this is needed in broadcasting when one of dimensions has length 0
# as then broadcasting produces Matrix{Any} rather than Matrix{<:Pair}
broadcast_pair(df::DTable, @nospecialize(p::AbstractMatrix)) = isempty(p) ? [] : p


# Copied as is from DataFrames.jl
function broadcast_pair(df::DTable, @nospecialize(p::AbstractVecOrMat{<:Pair}))
    isempty(p) && return []
    need_broadcast = false

    src = first.(p)
    first_src = first(src)
    if first_src isa Union{BroadcastedInvertedIndex,
        BroadcastedSelector}
        if any(!=(first_src), src)
            throw(ArgumentError("when broadcasting column selector it must " *
                                "have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_src.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_src = new_names
    else
        new_src = src
    end

    second = last.(p)
    first_second = first(second)
    if first_second isa Union{BroadcastedInvertedIndex,
        BroadcastedSelector}
        if any(!=(first_second), second)
            throw(ArgumentError("when using broadcasted column selector it " *
                                "must have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_second.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_second = new_names
    else
        if first_second isa Pair
            fun, dst = first_second
            if dst isa Union{BroadcastedInvertedIndex,
                BroadcastedSelector}
                if !all(x -> x isa Pair && last(x) == dst, second)
                    throw(ArgumentError("when using broadcasted column selector " *
                                        "it must have a constant value"))
                end
                need_broadcast = true
                new_names = names(df, dst.sel)
                if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
                    throw(ArgumentError("broadcasted dimension does not match the " *
                                        "number of selected columns"))
                end
                new_dst = new_names
                new_second = first.(second) .=> new_dst
            else
                new_second = second
            end
        else
            new_second = second
        end
    end

    if need_broadcast
        new_p = new_src .=> new_second
        return isempty(new_p) ? [] : new_p
    else
        return p
    end
end

# Copied as is from DataFrames.jl
function manipulate(df::DTable, @nospecialize(cs...); copycols::Bool, keeprows::Bool, renamecols::Bool)
    cs_vec = []
    for v in cs
        if v isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    return _manipulate(df, Any[normalize_selection(index(df), make_pair_concrete(c), renamecols) for c in cs_vec],
        copycols, keeprows)
end

# Not copied - full custom implementation
function _manipulate(df::DTable, normalized_cs::Vector{Any}, copycols::Bool, keeprows::Bool)
    #########
    # STAGE 1: Spawning full column thunks - also multicolumn when needed (except identity)
    # These get saved later and used in last stages.
    #########
    colresults = Dict{Int,Any}()
    for (i, (colidx, (f, _))) in enumerate(normalized_cs)
        if !(colidx isa AsTable) && !(f isa ByRow) && f != identity
            if length(colidx) > 0
                cs = DTableColumn.(Ref(df), [colidx...])
                colresults[i] = Dagger.@spawn f(cs...)
            else
                colresults[i] = Dagger.@spawn f() # case of select(d, [] => fun)
            end
        end
    end

    #########
    # STAGE 2: Fetching full column thunks with result of length 1
    # These will be just injected as values in the mapping, because it's a vector full of these values
    #########

    colresults = Dict{Int,Any}(
        k => fetch(Dagger.spawn(length, v)) == 1 ? fetch(v) : v
        for (k, v) in colresults
    )

    mapmask = [
        haskey(colresults, x) && colresults[x] isa Dagger.EagerThunk
        for (x, _) in enumerate(normalized_cs)
    ]

    mappable_part_of_normalized_cs = filter(x -> !mapmask[x[1]], collect(enumerate(normalized_cs)))

    #########
    # STAGE 3: Mapping function (need to ensure this is compiled only once)
    # It's awful right now, but it covers all cases
    # Essentially we skip all the non-mappable stuff here
    #########

    rd = map(x -> select_rowfunction(x, mappable_part_of_normalized_cs, colresults), df)

    #########
    # STAGE 4: Preping for last stage - getting all the full column thunks with not 1 lengths
    #########
    cpcolresults = Dict{Int,Any}()

    for (k, v) in colresults
        if v isa Dagger.EagerThunk
            cpcolresults[k] = v
        end
    end

    for (_, v) in colresults
        if v isa Dagger.EagerThunk
            if fetch(Dagger.spawn(length, v)) != length(df)
                throw("result column is not the size of the table")
            end
        end
    end
    #########
    # STAGE 5: Fill columns - meaning the previously omitted full column tasks
    # will be now merged into the final DTable
    #########

    rd = fillcolumns(rd, cpcolresults, normalized_cs, chunk_lengths(df))

    return rd
end

# Not copied - full custom implementation
function manipulate(dt::DTable, args::AbstractVector{Int}; copycols::Bool, keeprows::Bool, renamecols::Bool)
    colidx = first(args)
    colname = Tables.columnnames(Tables.columns(dt))[colidx]
    map(r -> (; colname => Tables.getcolumn(r, colidx)), dt)
end

# Copied as is from DataFrames.jl
function manipulate(df::DTable, c::MultiColumnIndex; copycols::Bool, keeprows::Bool,
    renamecols::Bool)
    if c isa AbstractVector{<:Pair}
        return manipulate(df, c..., copycols=copycols, keeprows=keeprows,
            renamecols=renamecols)
    else
        return manipulate(df, index(df)[c], copycols=copycols, keeprows=keeprows,
            renamecols=renamecols)
    end
end

# Copied as is from DataFrames.jl
manipulate(df::DTable, c::ColumnIndex; copycols::Bool, keeprows::Bool, renamecols::Bool) =
    manipulate(df, Int[index(df)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)


"""
    select(df::DTable, args...; copycols::Bool=true, renamecols::Bool=true)

Create a new DTable that contains columns from `df` specified by `args` and return it.
The result is guaranteed to have the same number of rows as df, except when no columns
are selected (in which case the result has zero rows).

This operation is supposed to provide the same functionality and syntax as `DataFrames.select`,
but for DTable input. Most cases should be covered and the output should be exactly the
same as one obtained using DataFrames. In case of output differences or `args` causing errors
please file an issue with reproduction steps and data.

Please refer to DataFrames documentation for more details on usage.
"""
select(df::DTable, @nospecialize(args...); copycols::Bool=true, renamecols::Bool=true) =
    manipulate(df, map(x -> broadcast_pair(df, x), args)...,
        copycols=copycols, keeprows=true, renamecols=renamecols)
