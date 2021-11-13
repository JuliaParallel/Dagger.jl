function resolve_colnames(l, r, on)
    isnothing(on) && error("yeet")
    if on isa Symbol
        l_cmp_symbols = [on]
        r_cmp_symbols = [on]
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


function match_inner_indices(l, r, l_ind::NTuple{N,Int}, r_ind::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length) # these vectors will be at least this long
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




function match_inner_indices_lsorted_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}, runique::Bool) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length) # these vectors will be at least this long
    sizehint!(vr, l_length)


    ri = enumerate(Tables.rows(r))
    li = enumerate(Tables.rows(l))

    riter = iterate(ri)
    liter = iterate(li)
    while true
        if riter !== nothing
            ((iind, iel), rstate) = riter
        else
            break
        end

        if liter !== nothing
            ((oind, oel), lstate) = liter
        else
            break
        end

        cmp = compare_rows_lt(iel, oel, cmp_r, cmp_l)
        if cmp
            riter = iterate(ri, rstate)
            continue
        end

        cmp = compare_rows_lt(oel, iel, cmp_l, cmp_r)
        if cmp
            liter = iterate(li, lstate)
            continue
        end

        cmp = compare_rows_eq(oel, iel, cmp_l, cmp_r)
        if cmp
            push!(vl, oind)
            push!(vr, iind)
            if runique
                liter = iterate(li, lstate)
                continue
            else
                inner_riter = iterate(ri, rstate)
                while inner_riter !== nothing
                    ((i_iind, i_iel), i_rstate) = inner_riter
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

function match_inner_indices_runique(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length) # these vectors will be at least this long
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

function match_inner_indices_rsorted(l, r, cmp_l::NTuple{N,Int}, cmp_r::NTuple{N,Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length) # these vectors will be at least this long
    sizehint!(vr, l_length)

    for (oind, oel) in enumerate(Tables.rows(l))
        prev_cmp = false
        for (iind, iel) in enumerate(Tables.rows(r))
            cmp = compare_rows_eq(oel, iel, cmp_l, cmp_r)
            if cmp
                push!(vl, oind)
                push!(vr, iind)
            end
            prev_cmp && !cmp && break
            prev_cmp = cmp
        end
    end
    vl, vr
end


function find_outer_indices(d, inner_indices)
    s = Set(one(UInt):length(Tables.rows(d)))
    setdiff!(s, inner_indices)
end


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
        c = rcols[i]
        vectype = jointype == :innerjoin ? eltype(c) : Union{eltype(c),Missing}
        newc = Vector{vectype}(undef, fulllength)
        copyto!(newc, view(c, inner_r))
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
    test = false
    @inbounds for x = 1:N
        test |= Tables.getcolumn(o, cmp_l[x]) < Tables.getcolumn(i, cmp_r[x])
        test && break
    end
    test
end
