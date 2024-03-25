struct FetchAdaptor end
Adapt.adapt_storage(::FetchAdaptor, x::Chunk) = fetch(x)
Adapt.adapt_storage(::FetchAdaptor, x::DTask) = fetch(x)
Adapt.adapt_structure(::FetchAdaptor, A::AbstractArray) =
    map(x->Adapt.adapt(FetchAdaptor(), x), A)

"""
    Dagger.fetch_all(x)

Recursively fetches all `DTask`s and `Chunk`s in `x`, returning an equivalent
object. Useful for converting arbitrary Dagger-enabled objects into a
non-Dagger form.
"""
fetch_all(x) = Adapt.adapt(FetchAdaptor(), x)
