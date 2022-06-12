import DataFrames

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

DataFrames.ncol(d::DTable) = length(Tables.columns(d))
index(df::DTable) = DataFrames.Index(_columnnames_svector(df))
