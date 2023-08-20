struct RecordAdaptor
    tasks::Set{Any}
end
struct FetchAdaptor end
Adapt.adapt_storage(ra::RecordAdaptor, t::Thunk) = (push!(ra.tasks, t); t)
Adapt.adapt_storage(::FetchAdaptor, t::Thunk) = fetch(t)
