import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv, ne

### Scheduling ###

struct DatadepsArgSpec
    pos::Union{Int, Symbol}
    value_type::Type
    dep_mod::Any
    ainfo::AbstractAliasing
end
struct DTaskDAGID{id} end
struct DAGSpec
    g::SimpleDiGraph{Int}
    id_to_uid::Dict{Int, UInt}
    uid_to_id::Dict{UInt, Int}
    id_to_functype::Dict{Int, Type} # FIXME: DatadepsArgSpec
    id_to_argtypes::Dict{Int, Vector{DatadepsArgSpec}}
    id_to_scope::Dict{Int, AbstractScope}
    id_to_spec::Dict{Int, DTaskSpec}
    id_to_task::Dict{Int, DTask}
    DAGSpec() = new(SimpleDiGraph{Int}(),
                    Dict{Int, UInt}(), Dict{UInt, Int}(),
                    Dict{Int, Type}(),
                    Dict{Int, Vector{DatadepsArgSpec}}(),
                    Dict{Int, AbstractScope}(),
                    Dict{Int, DTaskSpec}(),
                    Dict{Int, DTask}())
end

abstract type DataDepsScheduler end