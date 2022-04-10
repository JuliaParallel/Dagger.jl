using InvertedIndices
import DataAPI: Between, All, Cols
using DataAPI
const SymbolOrString = Union{Symbol, AbstractString}
const ColumnIndex = Union{Signed, Unsigned, SymbolOrString}
const MultiColumnIndex = Union{AbstractVector, Regex, Not, Between, All, Cols, Colon}
const MULTICOLUMNINDEX_TUPLE = (:AbstractVector, :Regex, :Not, :Between, :All, :Cols, :Colon)

ncol(d::DTable) = length(Tables.columns(d))

struct ByRow{T} <: Function
    fun::T
end

# invoke the generic AbstractVector function to ensure function is called
# exactly once for each element
(f::ByRow)(cols::AbstractVector...) =
    invoke(map,
           Tuple{typeof(f.fun), ntuple(i -> AbstractVector, length(cols))...},
           f.fun, cols...)
(f::ByRow)(table::NamedTuple) = [f.fun(nt) for nt in Tables.namedtupleiterator(table)]

# add a method to funname defined in other/utils.jl
funname(row::ByRow) = funname(row.fun)

make_pair_concrete(@nospecialize(x::Pair)) =
    make_pair_concrete(x.first) => make_pair_concrete(x.second)
make_pair_concrete(@nospecialize(x)) = x

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
broadcast_pair(df::DTable, @nospecialize(p::AbstractMatrix)) =
    isempty(p) ? [] : p

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
    println(cs_vec)
    return _manipulate(df, Any[DataFrames.normalize_selection(index(df), make_pair_concrete(c), renamecols) for c in cs_vec],
                    copycols, keeprows)
end

function _manipulate(df::DTable, normalized_cs::Vector{Any}, copycols::Bool, keeprows::Bool)
    @assert !(df isa SubDataFrame && copycols==false)
    newdf = DataFrame()
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
    transformed_cols = Set{Symbol}()
    # we allow resizing newdf only if up to some point only scalars were put
    # in it. The moment we put any vector into newdf its number of rows becomes fixed
    # Also if keeprows is true then we make sure to produce nrow(df) rows so resizing
    # is not allowed
    allow_resizing_newdf = Ref(!keeprows)
    # keep track of the fact if single column transformation like
    # :x or :x => :y or :x => identity
    # should make a copy
    # this ensures that we store a column from a source data frame in a
    # destination data frame without copying at most once
    column_to_copy = copycols ? trues(ncol(df)) : falses(ncol(df))

    for nc in normalized_cs
        if nc isa AbstractVector{Int} # only this case is NOT considered to be a transformation
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                # as nc is a multiple column selection without transformations
                # we allow duplicate column names with selections applied earlier
                # and ignore them for convinience, to allow for e.g. select(df, :x1, :)
                if !hasproperty(newdf, newname)
                    # allow shortening to 0 rows
                    if allow_resizing_newdf[] && nrow(newdf) == 1
                        newdfcols = _columns(newdf)
                        for (i, col) in enumerate(newdfcols)
                            newcol = fill!(similar(col, nrow(df)), first(col))
                            firstindex(newcol) != 1 && _onebased_check_error()
                            newdfcols[i] = newcol
                        end
                    end
                    # here even if keeprows is true all is OK
                    newdf[!, newname] = column_to_copy[i] ? df[:, i] : df[!, i]
                    column_to_copy[i] = true
                    allow_resizing_newdf[] = false
                end
            end
        else
            println(Ref{Any}(nc), df, newdf, transformed_cols, copycols,
            allow_resizing_newdf, column_to_copy)
            select_transform!(Ref{Any}(nc), df, newdf, transformed_cols, copycols,
                              allow_resizing_newdf, column_to_copy)
        end
    end
    return newdf
end

# function manipulate(dfv::SubDataFrame, @nospecialize(args...); copycols::Bool, keeprows::Bool,
#                     renamecols::Bool)
#     if copycols
#         cs_vec = []
#         for v in args
#             if v isa AbstractVecOrMat{<:Pair}
#                 append!(cs_vec, v)
#             else
#                 push!(cs_vec, v)
#             end
#         end
#         return _manipulate(dfv, Any[normalize_selection(index(dfv),
#                                     make_pair_concrete(c), renamecols) for c in cs_vec],
#                            true, keeprows)
#     else
#         # we do not support transformations here
#         # newinds contains only indexing; making it Vector{Any} avoids some compilation
#         newinds = []
#         seen_single_column = Set{Int}()
#         for ind in args
#             if ind isa ColumnIndex
#                 ind_idx = index(dfv)[ind]
#                 if ind_idx in seen_single_column
#                     throw(ArgumentError("selecting the same column multiple times " *
#                                         "using Symbol, string or integer is not allowed " *
#                                         "($ind was passed more than once"))
#                 else
#                     push!(seen_single_column, ind_idx)
#                 end
#             else
#                 newind = normalize_selection(index(dfv), make_pair_concrete(ind), renamecols)
#                 if newind isa Pair
#                     throw(ArgumentError("transforming and renaming columns of a " *
#                                         "SubDataFrame is not allowed when `copycols=false`"))
#                 end
#                 push!(newinds, newind)
#             end
#         end
#         return view(dfv, :, Cols(newinds...))
#     end
# end

manipulate(df::DTable, args::AbstractVector{Int}; copycols::Bool, keeprows::Bool,
           renamecols::Bool) = println("$args")
    # DataFrame(_columns(df)[args], Index(_names(df)[args]), copycols=copycols)

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

# function manipulate(dfv::SubDataFrame, args::MultiColumnIndex;
#                     copycols::Bool, keeprows::Bool, renamecols::Bool)
#     if args isa AbstractVector{<:Pair}
#         return manipulate(dfv, args..., copycols=copycols, keeprows=keeprows,
#                           renamecols=renamecols)
#     else
#         return copycols ? dfv[:, args] : view(dfv, :, args)
#     end
# end
using DataFrames
index(df::DTable) = DataFrames.Index(_columnnames_svector(df))
manipulate(df::DTable, c::ColumnIndex; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    manipulate(df, Int[index(df)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)

# manipulate(dfv::SubDataFrame, c::ColumnIndex; copycols::Bool, keeprows::Bool,
#            renamecols::Bool) =
#     manipulate(dfv, Int[index(dfv)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)


select(df::DTable, @nospecialize(args...); copycols::Bool=true, renamecols::Bool=true) =
manipulate(df, map(x -> broadcast_pair(df, x), args)...,
           copycols=copycols, keeprows=true, renamecols=renamecols)