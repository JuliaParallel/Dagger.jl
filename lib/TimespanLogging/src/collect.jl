as_old_id(::Type{C}, id) where C <: LogCategory = id
as_old_data(::Type{C}, data) where C <: LogCategory = data

function as_old_event(ev::LegacyEvent)
    phase = ev.phase == 0x00 ? :start : :finish
    return Event{phase}(ev.category, ev.id, ev.timeline, ev.timestamp, ev.gc_num, EMPTY_PROF)
end

function as_old_event(ev::EventRecord{C}) where C <: LogCategory
    phase = ev.phase == 0x00 ? :start : :finish
    return Event{phase}(category_symbol(C),
                        as_old_id(C, ev.id),
                        as_old_data(C, ev.data),
                        ev.timestamp,
                        GC_PLACEHOLDER[],
                        EMPTY_PROF)
end

function _steal_typed!(olds::Vector{Event}, buf::ChunkList{E}) where E
    open, pub = steal!(buf)
    n = event_count(open, pub)
    n == 0 && return olds
    recs = Vector{E}(undef, n)
    empty!(recs)
    collect_events!(recs, open, pub)
    for ev in recs
        push!(olds, as_old_event(ev))
    end
    return olds
end

"""
    steal_all_old_events() -> Vector{Event}

Steal every thread's legacy and typed buffers on this process and project
them into the legacy `Event` shape, sorted by timestamp.
"""
function steal_all_old_events()
    olds = Event[]
    states = THREAD_STATES[]
    for i in 1:length(states)
        s = states[i]
        s === nothing && continue
        open, pub = steal!(s.legacy)
        if event_count(open, pub) > 0
            recs = LegacyEvent[]
            collect_events!(recs, open, pub)
            for ev in recs
                push!(olds, as_old_event(ev))
            end
        end
        for j in 1:length(s.typed)
            isassigned(s.typed, j) || continue
            buf = s.typed[j]
            buf === nothing && continue
            _steal_typed!(olds, buf)
        end
    end
    sort!(olds, by=e -> e.timestamp)
    return olds
end

"""
    steal_typed(::Type{C}) -> Vector{EventRecord}

Steal only category `C` from every thread (tests / typed consumers).
"""
function steal_typed(::Type{C}) where C <: LogCategory
    E = event_type(C)
    out = E[]
    states = THREAD_STATES[]
    for i in 1:length(states)
        s = states[i]
        s === nothing && continue
        id = Int(category_id(C)) + 1
        id > length(s.typed) && continue
        isassigned(s.typed, id) || continue
        buf = s.typed[id]
        buf === nothing && continue
        open, pub = steal!(buf::ChunkList{E})
        collect_events!(out, open, pub)
    end
    sort!(out, by=e -> e.timestamp)
    return out
end

function steal_legacy()
    out = LegacyEvent[]
    states = THREAD_STATES[]
    for i in 1:length(states)
        s = states[i]
        s === nothing && continue
        open, pub = steal!(s.legacy)
        collect_events!(out, open, pub)
    end
    sort!(out, by=e -> e.timestamp)
    return out
end

function consume_events(events::Vector{Event},
                        consumers::Dict{Symbol,Any},
                        aggregators::Dict{Symbol,Any}=Dict{Symbol,Any}())
    result = Dict{Symbol,Vector}()
    n = length(events)
    for name in keys(consumers)
        result[name] = Vector{Any}(undef, n)
    end
    for (i, ev) in enumerate(events)
        for (name, c) in consumers
            result[name][i] = try
                c(ev)
            catch err
                @error "Error during event consumption:" exception=(err, catch_backtrace())
                nothing
            end
        end
    end
    for (name, agg) in aggregators
        try
            agg(result)
        catch err
            @error "Error during log aggregation:" exception=(err, catch_backtrace())
        end
    end
    return result
end

function _get_logs_local(consumers::Dict{Symbol,Any},
                         aggregators::Dict{Symbol,Any})
    events = steal_all_old_events()
    return consume_events(events, consumers, aggregators)
end

function _map_workers(f, wkrs)
    result = Dict{Int,Any}()
    @sync for p in wkrs
        if p == myid()
            result[p] = f()
        else
            @async result[p] = remotecall_fetch(f, p)
        end
    end
    return result
end

function get_logs!(::ActiveLog; only_local=false)
    wkrs = only_local ? Int[myid()] : procs()
    consumers = INSTALLED_CONSUMERS[]
    aggregators = INSTALLED_AGGREGATORS[]
    raw = _map_workers(wkrs) do
        TimespanLogging._get_logs_local(consumers, aggregators)
    end
    return Dict{Int,Dict{Symbol,Vector}}(p => v for (p, v) in raw)
end
