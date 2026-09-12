"""
    LogCategory

Abstract supertype of statically declared log categories. Each concrete
category has a runtime-assigned `category_id`, a concrete `id_type`, and a
`category_symbol` used when projecting events back to the legacy `Event`
shape at collect time.
"""
abstract type LogCategory end

const _CATEGORY_LOCK = Threads.SpinLock()
const _CATEGORY_IDS = Dict{Type,UInt8}()
const _CATEGORY_TYPES = Type[]
const _CATEGORY_SYMBOLS = Symbol[]

function register_category!(::Type{C}, name::Symbol) where C <: LogCategory
    @lock _CATEGORY_LOCK begin
        haskey(_CATEGORY_IDS, C) && return _CATEGORY_IDS[C]
        length(_CATEGORY_TYPES) < 64 || error("TimespanLogging supports at most 64 categories")
        id = UInt8(length(_CATEGORY_TYPES))
        push!(_CATEGORY_TYPES, C)
        push!(_CATEGORY_SYMBOLS, name)
        _CATEGORY_IDS[C] = id
        return id
    end
end

"""
    category_id(::Type{C}) -> UInt8

Runtime slot for `C`. Assigned lazily on first use so declaring a category
at another package's toplevel does not mutate TimespanLogging during that
package's precompilation (those mutations would be discarded when
TimespanLogging loads from its own image).
"""
function category_id(::Type{C}) where C <: LogCategory
    haskey(_CATEGORY_IDS, C) && return _CATEGORY_IDS[C]
    return register_category!(C, category_symbol(C))
end

category_symbol(::Type{C}) where C <: LogCategory =
    error("unregistered log category $C")

id_type(::Type{C}) where C <: LogCategory = Any
data_type(::Type{C}) where C <: LogCategory = Any

"""
    @logcategory Name as=:symbol id=(field::T, ...) [data=T] [old_data=...]

Declare a `LogCategory` and its concrete id struct (`NameId`). `data` defaults
to `Any`. Use `data=Nothing` for heartbeat events so the event is `isbits`
and the per-thread buffer stays allocation-free.

`as_old_id` is generated from the id fields (a NamedTuple with the same
names). `old_data` is optional and only needed when start/finish pass a
bare value that legacy consumers expect wrapped:

- `old_data=:data` → `(;data=data)` when `data` is not already a NamedTuple
- `old_data=(:f, :result)` → `(;f=data, result=data)` likewise
"""
macro logcategory(name::Symbol, args...)
    sym = Symbol(lowercase(String(name)))
    id_fields = Tuple{Symbol,Any}[]
    data_ty = :Any
    old_data = nothing
    for arg in args
        if Meta.isexpr(arg, :(=))
            lhs, rhs = arg.args[1], arg.args[2]
            if lhs === :as
                rhs isa QuoteNode || error("@logcategory as= must be a Symbol")
                sym = rhs.value
            elseif lhs === :id
                id_fields = _parse_fields(rhs)
            elseif lhs === :data
                data_ty = rhs
            elseif lhs === :old_data
                old_data = rhs
            else
                error("@logcategory: unknown option $lhs")
            end
        else
            error("@logcategory: expected keyword assignments, got $arg")
        end
    end
    id_struct = Symbol(name, :Id)
    id_defs = Expr[]
    nt_kws = Expr[]
    for (fname, fty) in id_fields
        push!(id_defs, :($(esc(fname))::$(esc(fty))))
        push!(nt_kws, Expr(:kw, fname, :(id.$(fname))))
    end
    old_id_body = Expr(:tuple, Expr(:parameters, nt_kws...))
    old_data_def = _gen_as_old_data(name, old_data)
    quote
        struct $(esc(name)) <: $(TimespanLogging).LogCategory end
        struct $(esc(id_struct))
            $(id_defs...)
        end
        $(TimespanLogging).category_symbol(::Type{$(esc(name))}) = $(QuoteNode(sym))
        $(TimespanLogging).id_type(::Type{$(esc(name))}) = $(esc(id_struct))
        $(TimespanLogging).data_type(::Type{$(esc(name))}) = $(esc(data_ty))
        $(TimespanLogging).as_old_id(::Type{$(esc(name))}, id::$(esc(id_struct))) = $old_id_body
        $old_data_def
        $(esc(name))
    end
end

function _parse_fields(rhs)
    if rhs === :nothing || rhs == :(())
        return Tuple{Symbol,Any}[]
    end
    args = Meta.isexpr(rhs, :tuple) ? rhs.args : [rhs]
    fields = Tuple{Symbol,Any}[]
    for a in args
        Meta.isexpr(a, :(::)) || error("@logcategory id= expected name::T, got $a")
        push!(fields, (a.args[1], a.args[2]))
    end
    return fields
end

function _old_data_keys(old_data)
    old_data === nothing && return Symbol[]
    if old_data isa QuoteNode
        return Symbol[old_data.value]
    elseif old_data isa Symbol
        return Symbol[old_data]
    elseif Meta.isexpr(old_data, :tuple)
        keys = Symbol[]
        for a in old_data.args
            if a isa QuoteNode
                push!(keys, a.value)
            elseif a isa Symbol
                push!(keys, a)
            else
                error("@logcategory old_data= expected Symbol or tuple of Symbols, got $a")
            end
        end
        return keys
    else
        error("@logcategory old_data= expected Symbol or tuple of Symbols, got $old_data")
    end
end

function _gen_as_old_data(name, old_data)
    keys = _old_data_keys(old_data)
    isempty(keys) && return nothing
    nt_kws = [Expr(:kw, k, :data) for k in keys]
    nt = Expr(:tuple, Expr(:parameters, nt_kws...))
    quote
        function $(TimespanLogging).as_old_data(::Type{$(esc(name))}, data)
            data isa NamedTuple && return data
            data === nothing && return data
            return $nt
        end
    end
end

"""
    EventRecord{Cat, Id, D}

Typed record stored in a per-category chunk list. `phase` is `0x00` for
start and `0x01` for finish. When `Id` and `D` are `isbits`, the record
itself is `isbits` and occupies a packed slot in the chunk.
"""
struct EventRecord{Cat<:LogCategory, Id, D}
    phase::UInt8
    timestamp::UInt64
    id::Id
    data::D
end

event_type(::Type{C}) where C <: LogCategory =
    EventRecord{C, id_type(C), data_type(C)}

@inline function adapt_id(::Type{C}, id) where C
    T = id_type(C)
    id isa T && return id
    return convert(T, id)
end

@inline function adapt_data(::Type{C}, data) where C
    T = data_type(C)
    data isa T && return data
    return convert(T, data)
end
