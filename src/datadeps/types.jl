import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv, ne

### AOT DAG Scheduling ###

"""
    DatadepsArgSpec

The structural description of one task argument, as recorded in a `DAGSpec`:
its position, the concrete type of the value, the dependency modifier it was
accessed through, and the aliasing region it covers. Two argument specs match
when their positions, types and modifiers are equal and their aliasings are
`equivalent_structure`, which is what lets a schedule be reused across re-runs
with freshly-allocated (but identically-shaped) data.
"""
struct DatadepsArgSpec
    pos::Union{Int, Symbol}
    value_type::Type
    dep_mod::Any
    ainfo::AbstractAliasing
end

struct DTaskDAGID{id} end

"""
    DAGSpec

A structural snapshot of a Datadeps region's task graph, built before any task
is launched so that a scheduler can plan over the whole region ahead of time
(AOT) rather than one task at a time (JIT). Tasks are identified by a dense
integer id; `id_to_uid`/`uid_to_id` relate those to the `DTask`s they describe.

A `DAGSpec` is also the cache key for a computed schedule (see
`DAGSpecSchedule` and `datadeps_dag_equivalent`), so it records only structure
— types, positions, scopes and aliasing shapes — never addresses.
"""
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
