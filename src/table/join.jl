function resolve_colnames(l, r, on)
    isnothing(on) && ArgumentError("Missing join argument 'on'")
    if on isa Symbol
        l_cmp_symbols = [on]
        r_cmp_symbols = [on]
    elseif on isa Pair{Symbol,Symbol}
        l_cmp_symbols = getindex(on, 1)
        r_cmp_symbols = getindex(on, 2)
    elseif on isa Vector{Symbol}
        l_cmp_symbols = on
        r_cmp_symbols = on
    elseif on isa Vector{Pair{Symbol,Symbol}}
        l_cmp_symbols = getindex.(on, 1)
        r_cmp_symbols = getindex.(on, 2)
    end

    l_cols = Tables.columns(l)
    l_schema = Tables.schema(l_cols)
    cmp_l_indices = Tables.columnindex.(Ref(l_schema), l_cmp_symbols)
    other_l_indices = setdiff(1:length(l_cols), cmp_l_indices)
    cmp_l = NTuple{length(cmp_l_indices),Int}(cmp_l_indices)
    other_l = NTuple{length(other_l_indices),Int}(other_l_indices)

    r_cols = Tables.columns(r)
    r_schema = Tables.schema(r_cols)
    cmp_r_indices = Tables.columnindex.(Ref(r_schema), r_cmp_symbols)
    other_r_indices = setdiff(1:length(r_cols), cmp_r_indices)
    cmp_r = NTuple{length(cmp_r_indices),Int}(cmp_r_indices)
    other_r = NTuple{length(other_r_indices),Int}(other_r_indices)

    rnames = (name in l_schema.names ? Symbol(string(name) * "_2") : name for name in getindex.(Ref(r_schema.names), other_r_indices))
    final_nameset = (l_schema.names..., rnames...)

    final_nameset, other_l, other_r, cmp_l, cmp_r
end

"""
    match_inner_indices(l, r, l_ind::NTuple{N,Int}, r_ind::NTuple{N,Int})

Returns two vectors containing indices of matched rows.
Standard non-optimized use case.
"""
function match_inner_indices(l, r, l_ind::NTuple{N,Int}, r_ind::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length)
    sizehint!(vr, l_length)
    for (oind, oel) in enumerate(Tables.rows(l))
        for (iind, iel) in enumerate(Tables.rows(r))
            if compare_rows_eq(oel, iel, l_ind, r_ind)
                push!(vl, oind)
                push!(vr, iind)
            end
        end
    end
    vl, vr
end

"""
    match_inner_indices_lookup(l, lookup, l_ind::NTuple{N,Int})

Returns two vectors containing indices of matched rows.
Uses `lookup` to find the matching indices.

`lookup` needs to be a dict-like structure that contains keys in
form of a `Tuple` of all matching columns and values in form
of type `Vector{UInt}` containing the related row indices.
"""
function match_inner_indices_lookup(l, lookup, l_ind::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length)
    sizehint!(vr, l_length)

    row_tuple = (row, cols) -> ([Tables.getcolumn(row, x) for x in cols]...,)
    _evec = Vector{UInt}()
    for (oind, oel) in enumerate(Tables.rows(l))
        indices = get(lookup, row_tuple(oel, l_ind), _evec)
        for iind in indices
            push!(vl, oind)
            push!(vr, iind)
        end
    end
    vl, vr
end

"""
    match_inner_indices_lsorted_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}, runique::Bool)

Returns two vectors containing indices of matched rows.
Optimized pass for the left table sorted, right table sorted and optionally right table only containing unique keys.
"""
function match_inner_indices_lsorted_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}, runique::Bool) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length)
    sizehint!(vr, l_length)

    ri = enumerate(Tables.rows(r))
    li = enumerate(Tables.rows(l))

    riter = iterate(ri)
    liter = iterate(li)
    while true
        if riter === nothing || liter === nothing
            break
        end

        (iind, iel), rstate = riter
        (oind, oel), lstate = liter

        if compare_rows_lt(iel, oel, cmp_r, cmp_l)
            riter = iterate(ri, rstate)
            continue
        end

        if compare_rows_lt(oel, iel, cmp_l, cmp_r)
            liter = iterate(li, lstate)
            continue
        end

        if compare_rows_eq(oel, iel, cmp_l, cmp_r)
            push!(vl, oind)
            push!(vr, iind)
            if runique
                liter = iterate(li, lstate)
                continue
            else
                inner_riter = iterate(ri, rstate)
                while inner_riter !== nothing
                    (i_iind, i_iel), i_rstate = inner_riter
                    if compare_rows_eq(oel, i_iel, cmp_l, cmp_r)
                        push!(vl, oind)
                        push!(vr, i_iind)
                        inner_riter = iterate(ri, i_rstate)
                    else
                        break
                    end
                end
                liter = iterate(li, lstate)
            end
        end
    end
    vl, vr
end

"""
    match_inner_indices_runique(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int})

Returns two vectors containing indices of matched rows.
Optimized pass for joins with the right table containing unique keys only.
"""
function match_inner_indices_runique(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length)
    sizehint!(vr, l_length)

    for (oind, oel) in enumerate(Tables.rows(l))
        for (iind, iel) in enumerate(Tables.rows(r))
            if compare_rows_eq(oel, iel, cmp_l, cmp_r)
                push!(vl, oind)
                push!(vr, iind)
                break
            end
        end
    end
    vl, vr
end

"""
    match_inner_indices_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int})

Returns two vectors containing indices of matched rows.
Optimized pass for joins with a sorted right table.
"""
function match_inner_indices_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length)
    sizehint!(vr, l_length)

    for (oind, oel) in enumerate(Tables.rows(l))
        for (iind, iel) in enumerate(Tables.rows(r))
            if compare_rows_lt(oel, iel, cmp_l, cmp_r)
                break
            elseif compare_rows_eq(oel, iel, cmp_l, cmp_r)
                push!(vl, oind)
                push!(vr, iind)
            end
        end
    end
    vl, vr
end

"""
    find_outer_indices(d, inner_indices)

Finds the unmatched indices from the table.
"""
function find_outer_indices(d, inner_indices)
    s = Set(one(UInt):length(Tables.rows(d)))
    setdiff!(s, inner_indices)
end

"""
    build_joined_table(jointype, names, l, r, inner_l, inner_r, outer_l, other_r)

Takes the indices of matching rows (`inner*`) and the ones that weren't matched (`outer_l`) from the `l` table
and builds the result based on that.

Uses all the columns from the left column and the `other_r` columns from the right table.
"""
function build_joined_table(
        jointype::Symbol,
        names::Tuple,
        l,
        r,
        inner_l::Vector{UInt},
        inner_r::Vector{UInt},
        outer_l::Set{UInt},
        other_r::NTuple,
    )

    fulllength = length(inner_l) + length(outer_l)

    cols = Vector{AbstractVector}(undef, length(names))

    colcounter = one(Int)

    for c in Tables.columns(l)
        newc = Vector{eltype(c)}(undef, fulllength)
        copyto!(newc, view(c, inner_l))
        copyto!(newc, length(inner_l) + 1, view(c, collect(outer_l)))
        cols[colcounter] = newc
        colcounter += 1
    end

    rcols = Tables.columns(r)
    for i in other_r
        t = Tables.schema(rcols).types[i]
        vectype = jointype == :innerjoin ? t : Union{t,Missing}
        newc = Vector{vectype}(undef, fulllength)
        if length(inner_r) > 0 # skip fetching and copying if there's no records matched
            c = Tables.getcolumn(rcols, i)
            copyto!(newc, view(c, inner_r))
        end
        cols[colcounter] = newc
        colcounter += 1
    end

    sink = Tables.materializer(l)
    sink((; zip(names, cols)...))
end


@inline function compare_rows_eq(o, i, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    test = true
    @inbounds for x = 1:N
        test &= Tables.getcolumn(o, cmp_l[x]) == Tables.getcolumn(i, cmp_r[x])
        test || break
    end
    test
end


@inline function compare_rows_lt(o, i, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    @inbounds for x = 1:N
        l = Tables.getcolumn(o, cmp_l[x])
        r = Tables.getcolumn(i, cmp_r[x])
        if l > r
            return false
        elseif l < r
            return true
        end
    end
    false
end

