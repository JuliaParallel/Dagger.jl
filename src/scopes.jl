export AnyScope, UnionScope, NodeScope, ProcessScope, ExactScope

abstract type AbstractScope end

"Default scope that is unconstrained."
struct AnyScope <: AbstractScope end

"Union of two or more scopes."
struct UnionScope <: AbstractScope
    scopes::Tuple
end
UnionScope(scopes...) = UnionScope((scopes...,))
UnionScope(scopes::Vector{<:AbstractScope}) = UnionScope((scopes...,))
UnionScope(s::AbstractScope) = UnionScope((s,))
UnionScope() = throw(ArgumentError("Cannot construct empty UnionScope"))

"Scoped to the same physical node."
struct NodeScope <: AbstractScope
    uuid::UUID
end
NodeScope() = NodeScope(SYSTEM_UUIDS[myid()])

"Scoped to the same OS process."
struct ProcessScope <: AbstractScope
    parent::NodeScope
    wid::Int
end
function ProcessScope(wid::Integer)
    if wid == myid()
        ProcessScope(NodeScope(), wid)
    else
        ProcessScope(NodeScope(system_uuid(wid)), wid)
    end
end
ProcessScope(p::OSProc) = ProcessScope(p.pid)
ProcessScope() = ProcessScope(myid())

"Scoped to a specific processor."
struct ExactScope <: AbstractScope
    parent::ProcessScope
    processor::Processor
end
ExactScope(proc) = ExactScope(ProcessScope(get_parent(proc).pid), proc)

"Indicates that the applied scopes `x` and `y` are incompatible."
struct InvalidScope <: AbstractScope
    x::AbstractScope
    y::AbstractScope
end

# Comparisons and constraint checking

constrain(x, y) = x < y ? constrain(y, x) : throw(MethodError(constrain, x, y))

Base.isless(::AnyScope, ::AnyScope) = false
Base.isless(::AnyScope, x) = false
Base.isless(x, ::AnyScope) = true
constrain(::AnyScope, ::AnyScope) = AnyScope()
constrain(::AnyScope, y) = y

Base.isless(::UnionScope, ::UnionScope) = false
Base.isless(::UnionScope, ::AnyScope) = true
function constrain(x::UnionScope, y::UnionScope)
    zs = Vector{AbstractScope}()
    for xs in x.scopes
        for ys in y.scopes
            scope = constrain(xs, ys)
            scope isa InvalidScope && continue
            push!(zs, scope)
        end
    end
    isempty(zs) && return InvalidScope(x, y)
    UnionScope(zs)
end
constrain(x::UnionScope, y) = constrain(x, UnionScope((y,)))

Base.isless(::NodeScope, ::NodeScope) = false
Base.isless(::NodeScope, ::AnyScope) = true
constrain(x::NodeScope, y::NodeScope) =
    x == y ? y : InvalidScope(x, y)

Base.isless(::ProcessScope, ::ProcessScope) = false
Base.isless(::ProcessScope, ::NodeScope) = true
Base.isless(::ProcessScope, ::AnyScope) = true
constrain(x::ProcessScope, y::ProcessScope) =
    x == y ? y : InvalidScope(x, y)
constrain(x::NodeScope, y::ProcessScope) =
    x == y.parent ? y : InvalidScope(x, y)

Base.isless(::ExactScope, ::ExactScope) = false
Base.isless(::ExactScope, ::ProcessScope) = true
Base.isless(::ExactScope, ::NodeScope) = true
Base.isless(::ExactScope, ::AnyScope) = true
constrain(x::ExactScope, y::ExactScope) =
    x == y ? y : InvalidScope(x, y)
constrain(x::ProcessScope, y::ExactScope) =
    x == y.parent ? y : InvalidScope(x, y)
constrain(x::NodeScope, y::ExactScope) =
    x == y.parent.parent ? y : InvalidScope(x, y)
