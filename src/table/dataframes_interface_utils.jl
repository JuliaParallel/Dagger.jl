import DataAPI: ncol
import DataFrames: Index, ByRow, AsTable

function select_rowfunction(row, mappable_part_of_normalized_cs, colresults)
    _cs = [
        begin
            kk = result_colname === AsTable ? Symbol("AsTable$(i)") : result_colname
            vv = begin
                args = if colidx isa AsTable
                    (; [
                        k => Tables.getcolumn(row, k)
                        for k in getindex.(Ref(Tables.columnnames(row)), colidx.cols)
                    ]...)
                else
                    Tables.getcolumn.(Ref(row), colidx)
                end

                if f isa ByRow && !(colidx isa AsTable) && length(colidx) == 0
                    f.fun()
                elseif f isa ByRow
                    f.fun(args)
                elseif f == identity
                    args
                elseif length(colresults[i]) == 1
                    colresults[i]
                else
                    throw(ErrorException("Weird unhandled stuff"))
                end
            end
            kk => vv
        end
        for (i, (colidx, (f, result_colname))) in mappable_part_of_normalized_cs
    ]
    return (; _cs...)
end

function fillcolumns(dt::DTable, ics::Dict{Int,Any}, normalized_cs, chunk_lengths_of_original_dt::Vector{Int})
    col_keys_indices = collect(keys(ics))::Vector{Int}
    col_vecs = map(x -> ics[x], col_keys_indices)::Union{Vector{Any},Vector{Dagger.EagerThunk}}

    f = (ch, csymbols, colfragments) -> begin
        col_vecs_fetched = fetch.(colfragments)
        colnames = Vector{Symbol}()
        cols = Vector{Any}()
        last_astable = 0

        for (idx, (_,(_, sym))) in enumerate(normalized_cs)
            if sym !== AsTable
                col = if sym in csymbols
                    index = something(indexin(csymbols, [sym])...)
                    col_vecs_fetched[index]
                else
                    Tables.getcolumn(ch, sym)
                end
                push!(colnames, sym)
                push!(cols, col)
            elseif sym === AsTable
                i = findfirst(x -> x === AsTable, csymbols[(last_astable + 1):end])
                if i === nothing
                    c = Tables.getcolumn(ch, Symbol("AsTable$(idx)"))
                else
                    last_astable = i
                    c = col_vecs_fetched[i]
                end

                push!.(
                    Ref(colnames),
                    Tables.columnnames(Tables.columns(c))
                )
                push!.(
                    Ref(cols),
                    Tables.getcolumn.(
                        Ref(Tables.columns(c)),
                        Tables.columnnames(Tables.columns(c))
                    )
                )
            else
                throw(ErrorException("something is off"))
            end
        end
        Tables.materializer(ch)(
            merge(
                NamedTuple(),
                (; [e[1] => e[2] for e in zip(colnames, cols)]...)
            )
        )
    end

    colfragment = (column, s, e) -> Dagger.@spawn getindex(column, s:e)
    clenghts = chunk_lengths_of_original_dt
    result_column_symbols = getindex.(Ref(map(x -> x[2][2], normalized_cs)), col_keys_indices)

    chunks = [
        begin
            cfrags = [
                colfragment(column, 1 + sum(clenghts[1:(i - 1)]), sum(clenghts[1:i]))
                for column in col_vecs
            ]
            Dagger.@spawn f(ch, result_column_symbols, cfrags)
        end
        for (i, ch) in enumerate(dt.chunks)
    ]

    DTable(chunks, dt.tabletype)
end

ncol(d::DTable) = length(Tables.columns(d))
index(df::DTable) = Index(_columnnames_svector(df))
