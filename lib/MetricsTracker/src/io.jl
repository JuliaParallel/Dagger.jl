struct JournalEntry
    timestamp::Float64
    context::ContextKey
    metric::AbstractMetric
    key::Any
    value::Any
end

mutable struct MetricsJournal
    const path::String
    const base_path::String
    @atomic enabled::Bool
    @atomic stream::Union{IOStream, Nothing}
    write_lock::ReentrantLock
    @atomic write_count::UInt64
    @atomic compact_threshold::UInt64

    function MetricsJournal(path::String; compact_threshold::UInt64=UInt64(1024))
        base_path = path * ".base"
        return new(path, base_path, false, nothing, ReentrantLock(), UInt64(0), compact_threshold)
    end
end

function open_journal!(journal::MetricsJournal)
    lock(journal.write_lock) do
        s = @atomic journal.stream
        if s !== nothing
            return journal
        end
        new_stream = open(journal.path, "a")
        @atomic journal.stream = new_stream
        @atomic journal.enabled = true
        return journal
    end
    return journal
end

function close_journal!(journal::MetricsJournal)
    lock(journal.write_lock) do
        @atomic journal.enabled = false
        s = @atomic journal.stream
        if s !== nothing
            try
                flush(s)
                close(s)
            catch
            end
            @atomic journal.stream = nothing
        end
        return journal
    end
    return journal
end

function append_journal!(journal::MetricsJournal, context::ContextKey,
                         metric::AbstractMetric, key, value)
    (@atomic journal.enabled) || return
    entry = JournalEntry(time(), context, metric, key, value)
    lock(journal.write_lock) do
        s = @atomic journal.stream
        s === nothing && return
        try
            serialize(s, entry)
            flush(s)
            @atomic journal.write_count += UInt64(1)
        catch err
            @atomic journal.enabled = false
            @warn "MetricsJournal write failed, disabling" exception=err
        end
        return
    end
    return
end

function load_journal_entries(path::String)
    entries = JournalEntry[]
    isfile(path) || return entries
    open(path, "r") do io
        while !eof(io)
            try
                entry = deserialize(io)::JournalEntry
                push!(entries, entry)
            catch err
                @warn "Stopping journal replay at corrupted entry" exception=err
                break
            end
        end
    end
    return entries
end

function load_base_snapshot(path::String)
    isfile(path) || return nothing
    try
        return deserialize(path)::Dict{ContextKey, AbstractContextStorage}
    catch err
        @warn "Failed to load base snapshot" exception=err
        return nothing
    end
end

function atomic_write(f::Function, path::String)
    tmp_path = path * ".tmp." * string(getpid()) * "." * string(rand(UInt32); base=16)
    open(tmp_path, "w") do io
        f(io)
    end
    Base.Filesystem.rename(tmp_path, path)
    return
end

function write_base_snapshot(path::String, contexts::Dict{ContextKey, AbstractContextStorage})
    atomic_write(path) do io
        serialize(io, contexts)
    end
    return
end

const JOURNALS = Base.Lockable(WeakKeyDict{MetricsCache, MetricsJournal}())

function attach_journal!(cache::MetricsCache, path::String;
                         compact_threshold::UInt64=UInt64(1024))
    journal = MetricsJournal(path; compact_threshold)
    existing = lock(JOURNALS) do dict
        prev = get(dict, cache, nothing)
        dict[cache] = journal
        return prev
    end
    if existing !== nothing
        close_journal!(existing)
    end
    open_journal!(journal)
    return journal
end

function detach_journal!(cache::MetricsCache)
    journal = lock(JOURNALS) do dict
        prev = get(dict, cache, nothing)
        if prev !== nothing
            delete!(dict, cache)
        end
        return prev
    end
    if journal !== nothing
        close_journal!(journal)
    end
    return journal
end

function get_journal(cache::MetricsCache)
    return lock(JOURNALS) do dict
        get(dict, cache, nothing)
    end
end

function load_metrics!(cache::MetricsCache, path::String)
    base_path = path * ".base"
    base = load_base_snapshot(base_path)
    journal_entries = load_journal_entries(path)
    bulk_update!(cache) do c
        if base !== nothing
            for (ctx_key, ctx) in base
                K = key_type(ctx)
                dest_ctx = pending_context!(c, ctx_key[1], ctx_key[2], K)
                for (metric, storage) in ctx.storages
                    dest_storage = get_or_create_storage!(dest_ctx, metric)
                    for k in storage.insertion_order
                        set_metric_value!(dest_storage, k, storage.data[k])
                    end
                end
            end
        end
        for entry in journal_entries
            K = typeof(entry.key)
            ctx = pending_context!(c, entry.context[1], entry.context[2], K)
            storage = get_or_create_storage!(ctx, entry.metric)
            set_metric_value!(storage, entry.key, entry.value)
        end
    end
    return cache
end

function load_metrics!(path::String)
    return load_metrics!(GLOBAL_METRICS_CACHE, path)
end

function save_metrics!(cache::MetricsCache, path::String)
    snap = snapshot(cache)
    base_path = path * ".base"
    write_base_snapshot(base_path, snap.contexts)
    journal = get_journal(cache)
    if journal !== nothing
        lock(journal.write_lock) do
            s = @atomic journal.stream
            if s !== nothing
                try
                    flush(s)
                    close(s)
                catch
                end
            end
            try
                isfile(journal.path) && rm(journal.path)
            catch
            end
            new_stream = open(journal.path, "a")
            @atomic journal.stream = new_stream
            @atomic journal.write_count = UInt64(0)
            return
        end
    end
    return path
end

function save_metrics!(path::String)
    return save_metrics!(GLOBAL_METRICS_CACHE, path)
end

function compact_journal!(cache::MetricsCache)
    journal = get_journal(cache)
    journal === nothing && return
    if (@atomic journal.write_count) >= (@atomic journal.compact_threshold)
        save_metrics!(cache, journal.path)
    end
    return
end
