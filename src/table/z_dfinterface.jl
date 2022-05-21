using InvertedIndices
import DataAPI: Between, All, Cols
using DataAPI
using DataFrames

import DataFrames: SymbolOrString, ColumnIndex, MultiColumnIndex, MULTICOLUMNINDEX_TUPLE, ByRow, funname, make_pair_concrete, AsTable, ncol

#################


mutable struct DTableColumn{T,TT}
    dtable::DTable
    current_chunk::Int
    col::Int
    colname::Symbol
    chunk_lengths::Vector{Int}
    current_iterator::Union{Nothing, TT}
    chunkstore::Union{Nothing,Vector{T}}
end

__ff = (ch,col) -> Tables.getcolumn(Tables.columns(ch), col)

function DTableColumn(dtable::DTable, col::Int)
    column_eltype = Tables.schema(Tables.columns(dtable)).types[col]
    iterator_type = fetch(Dagger.spawn((ch,_col) -> typeof(iterate(__ff(ch,_col))), dtable.chunks[1], col))

    DTableColumn{column_eltype, iterator_type}(
        dtable,
        0,
        col,
        _columnnames_svector(dtable)[col],
        chunk_lengths(dtable),
        nothing,
        nothing,
    )
end


function getindex(dtablecolumn::DTableColumn, idx::Int)
    chunk_idx = 0
    s = 1
    for (i, e) in enumerate(dtablecolumn.chunk_lengths)
        if s <= idx < s + e
            chunk_idx = i
            break
        end
        s=s+e
    end
    chunk_idx == 0 && throw(BoundsError())
    offset = idx - s + 1
    chunk = fetch(Dagger.spawn(__ff, dtablecolumn.dtable.chunks[chunk_idx], dtablecolumn.col))

    row, iter = iterate(Tables.rows(chunk))
    for _ in 1:(offset - 1)
        row, iter = iterate(Tables.rows(chunk), iter)
    end
    Tables.getcolumn(row, dtablecolumn.col)
end

length(dtablecolumn::DTableColumn) = sum(dtablecolumn.chunk_lengths)


function pull_next_chunk(dtablecolumn::DTableColumn, chunkidx::Int)
    while dtablecolumn.current_iterator === nothing
        chunkidx += 1
        if chunkidx <= length(dtablecolumn.dtable.chunks)
            dtablecolumn.chunkstore =
                fetch(Dagger.spawn(__ff, dtablecolumn.dtable.chunks[chunkidx], dtablecolumn.col))
        else
            return chunkidx
        end
        dtablecolumn.current_iterator = iterate(dtablecolumn.chunkstore)
    end
    return chunkidx
end


function iterate(dtablecolumn::DTableColumn)
    if length(dtablecolumn) == 0
        return nothing
    end
    dtablecolumn.chunkstore = nothing
    dtablecolumn.current_iterator = nothing
    chunkidx = pull_next_chunk(dtablecolumn, 0)
    ci = dtablecolumn.current_iterator
    if ci === nothing
        return nothing
    else
        return (ci[1], (chunkidx, ci[2]))
    end
end

function iterate(dtablecolumn::DTableColumn, iter)
    (chunkidx, i) = iter
    cs = dtablecolumn.chunkstore
    ci = nothing
    if cs !== nothing
        ci = iterate(cs, i)
    else
        return nothing
    end
    dtablecolumn.current_iterator = ci
    chunkidx = pull_next_chunk(dtablecolumn, chunkidx)
    ci = dtablecolumn.current_iterator
    if ci === nothing
        return nothing
    else
        return (ci[1], (chunkidx, ci[2]))
    end
end

################################

function fillcolumn(dt::DTable, index::Int, column)
    csymbol = _columnnames_svector(dt)[index]
    f = (ch, colfragment) -> begin
        Tables.materializer(ch)(
            merge(
                Tables.columntable(ch),
                (; [csymbol => colfragment]...)
            )
        )
    end
    colfragment = (column, s, e) -> Dagger.@spawn getindex(column, s:e)
    clenghts = chunk_lengths(dt)
    chunks = [
        begin
            cfrag = colfragment(column, 1 + sum(clenghts[1:(i-1)]), sum(clenghts[1:i]))
            Dagger.@spawn f(ch, cfrag)
        end
        for (i, ch) in enumerate(dt.chunks)
    ]
    DTable(chunks, dt.tabletype)
end

function fillcolumns(dt::DTable, ics, normalized_cs)
    ks = [k for k in keys(ics)]
    vs = map(x -> ics[x], ks)

    f = (ch, csymbols, colfragments) -> begin
        cf = fetch.(colfragments)
        colnames = []
        cols = []
        last_astable = 0
        for (idx, (_,(_, sym))) in enumerate(normalized_cs)
            if sym !== AsTable
                push!(colnames, sym)
                col = sym in csymbols ?
                    cf[something(indexin(csymbols, [sym])...)] :
                    Tables.getcolumn(ch, sym)
                push!(cols, col)
            elseif sym === AsTable
                i = findfirst(x->x===AsTable, csymbols[last_astable+1:end])
                if i === nothing
                    c = Tables.getcolumn(ch, Symbol("AsTable$(idx)"))
                else
                    last_astable=i
                    c = cf[i]
                end
                push!.(Ref(colnames),Tables.columnnames(Tables.columns(c)))
                push!.(Ref(cols), Tables.getcolumn.(Ref(Tables.columns(c)), Tables.columnnames(Tables.columns(c))))
            end
        end

        Tables.materializer(ch)(
            merge(
                NamedTuple(),
                (; [e[1] => e[2] for e in zip(colnames,cols)]...)
            )
        )
    end
    colfragment = (column, s, e) -> Dagger.@spawn getindex(column, s:e)
    clenghts = chunk_lengths(dt)

    _csymbols = getindex.(Ref(map(x->x[2][2],normalized_cs)), ks)
    chunks = [
        begin
            cfrags = [colfragment(column, 1 + sum(clenghts[1:(i-1)]), sum(clenghts[1:i])) for column in vs]
            Dagger.@spawn f(ch, _csymbols, cfrags)
        end
        for (i, ch) in enumerate(dt.chunks)
    ]

    DTable(chunks, dt.tabletype)
end

DataFrames.ncol(d::DTable) = length(Tables.columns(d))

broadcast_pair(df::DTable, @nospecialize(p::Any)) = p

function broadcast_pair(df::DTable, @nospecialize(p::Pair))
    src, second = p
    src_broadcast = src isa Union{InvertedIndices.BroadcastedInvertedIndex,
        DataAPI.BroadcastedSelector}
    second_broadcast = second isa Union{InvertedIndices.BroadcastedInvertedIndex,
        DataAPI.BroadcastedSelector}
    if second isa Pair
        fun, dst = second
        dst_broadcast = dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
            DataAPI.BroadcastedSelector}
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

function broadcast_pair(df::DTable, @nospecialize(p::AbstractVecOrMat{<:Pair}))
    isempty(p) && return []
    need_broadcast = false

    src = first.(p)
    first_src = first(src)
    if first_src isa Union{InvertedIndices.BroadcastedInvertedIndex,
        DataAPI.BroadcastedSelector}
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
    if first_second isa Union{InvertedIndices.BroadcastedInvertedIndex,
        DataAPI.BroadcastedSelector}
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
            if dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
                DataAPI.BroadcastedSelector}
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

function manipulate(df::DTable, @nospecialize(cs...); copycols::Bool, keeprows::Bool, renamecols::Bool)
    cs_vec = []
    for v in cs
        if v isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    # println(cs_vec)
    return _manipulate(df, Any[DataFrames.normalize_selection(index(df), make_pair_concrete(c), renamecols) for c in cs_vec],
        copycols, keeprows)
end


function _manipulate(df::DTable, normalized_cs::Vector{Any}, copycols::Bool, keeprows::Bool)
    ############ DTABLE SPECIFIC
    # println.(normalized_cs)

    #########
    # STAGE 1: Spawning full column thunks - also multicolumn when needed (except identity)
    # These get saved later and used in last stages.
    #########
    colresults = Dict{Int,Any}()
    for (i, (colidx, (f, result_colname))) in enumerate(normalized_cs)
        if !(colidx isa AsTable) && !(f isa ByRow) && f != identity
            cs = DTableColumn.(Ref(df), [colidx...])
            colresults[i] = Dagger.@spawn f(cs...)
        end
    end
    
    #########
    # STAGE 2: Fetching full column thunks with result of length 1
    # These will be just injected as values in the mapping, because it's a vector full of these values
    #########
    colresults = Dict(k => fetch(Dagger.spawn(length, v)) == 1 ? fetch(v) : v for (k, v) in colresults)

    dtlen = length(df)
    mapmask = [haskey(colresults,x) && colresults[x] isa Dagger.EagerThunk for (x,_) in enumerate(normalized_cs)]


    #########
    # STAGE 3: Mapping function (need to ensure this is compiled only once)
    # It's awful right now, but it covers all cases
    # Essentially we skip all the non-mappable stuff here
    #########
    rowfunction = (row) -> begin
        (; [
            (result_colname === AsTable ? Symbol("AsTable$(i)") : result_colname ) => begin

                args = if colidx isa AsTable
                    (; [k => Tables.getcolumn(row, k) for k in getindex.(Ref(Tables.columnnames(row)), colidx.cols)]...)
                else
                    Tables.getcolumn.(Ref(row), colidx)
                end

                if f isa ByRow
                    f.fun(args)
                elseif f == identity
                    args
                elseif !(colresults[i] isa Dagger.EagerThunk) && length(colresults[i]) == 1
                    colresults[i]
                elseif colresults[i] isa Dagger.EagerThunk #this is skipped actually
                    nothing
                end
                kk => vv
            end
            for (i, (colidx, (f, result_colname))) in filter(x-> !mapmask[x[1]], collect(enumerate(normalized_cs)))
        ])
        return (; _cs...)
    end

    rd = map(rowfunction, df)

    #########
    # STAGE 4: Preping for last stage - getting all the full column thunks with not 1 lengths
    #########
    cpcolresults = Dict()

    for (k,v) in colresults
        if v isa Dagger.EagerThunk
            cpcolresults[k] = v
        end
    end

    # LENGTH CHECK!!!
    for (k, v) in colresults
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
    rd = fillcolumns(rd, cpcolresults,normalized_cs)
    return rd

    ########### end DTABLE SPECIFIC


    # the role of transformed_cols is the following
    # * make sure that we do not use the same target column name twice in transformations;
    #   note though that it can appear in no-transformation selection like
    #   `select(df, :, :a => ByRow(sin) => :a), where :a is produced both by `:`
    #   and by `:a => ByRow(sin) => :a`
    # * make sure that if some column is produced by transformation like
    #   `:a => ByRow(sin) => :a` and it appears earlier or later in non-transforming
    #   selection like `:` or `:a` then the transformation is computed and inserted
    #   in to the target data frame once and only once the first time the target column
    #   is requested to be produced.
    #
    # For example in:
    #
    # julia> df = DataFrame(a=1:2, b=3:4)
    # 2×2 DataFrame
    #  Row │ a      b
    #      │ Int64  Int64
    # ─────┼──────────────
    #    1 │     1      3
    #    2 │     2      4
    #
    # julia> select(df, :, :a => ByRow(sin) => :a)
    # 2×2 DataFrame
    #  Row │ a         b
    #      │ Float64   Int64
    # ─────┼─────────────────
    #    1 │ 0.841471      3
    #    2 │ 0.909297      4
    #
    # julia> select(df, :, :a => ByRow(sin) => :a, :a)
    # ERROR: ArgumentError: duplicate output column name: :a
    #
    # transformed_cols keeps a set of columns that were generated via a transformation
    # up till the point. Note that single column selection and column renaming is
    # considered to be a transformation
    # transformed_cols = Set{Symbol}()
    # we allow resizing newdf only if up to some point only scalars were put
    # in it. The moment we put any vector into newdf its number of rows becomes fixed
    # Also if keeprows is true then we make sure to produce nrow(df) rows so resizing
    # is not allowed
    # allow_resizing_newdf = Ref(!keeprows)
    # keep track of the fact if single column transformation like
    # :x or :x => :y or :x => identity
    # should make a copy
    # this ensures that we store a column from a source data frame in a
    # destination data frame without copying at most once
    # column_to_copy = copycols ? trues(ncol(df)) : falses(ncol(df))
end

function manipulate(dt::DTable, args::AbstractVector{Int}; copycols::Bool, keeprows::Bool, renamecols::Bool)
    # this is for single arg Int e.g. Dagger.select(dt, 2)
    # DataFrame(_columns(df)[args], Index(_names(df)[args]), copycols=copycols)

    ################## DTABLE SPECIFIC
    colidx = first(args)
    colname = Tables.columnnames(Tables.columns(dt))[colidx]
    map(r -> (; colname => Tables.getcolumn(r, colidx)), dt)
    ################## DTABLE SPECIFIC
end

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

index(df::DTable) = DataFrames.Index(_columnnames_svector(df))

manipulate(df::DTable, c::ColumnIndex; copycols::Bool, keeprows::Bool, renamecols::Bool) =
    manipulate(df, Int[index(df)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)

select(df::DTable, @nospecialize(args...); copycols::Bool=true, renamecols::Bool=true) =
    manipulate(df, map(x -> broadcast_pair(df, x), args)...,
        copycols=copycols, keeprows=true, renamecols=renamecols)
