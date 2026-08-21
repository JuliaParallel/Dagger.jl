struct Signature
    sig::Vector{Any}#DataType}
    hash::UInt
    sig_nokw::SubArray{Any,1,Vector{Any},Tuple{UnitRange{Int}},true}
    hash_nokw::UInt
    function Signature(sig::Vector{Any})#DataType})
        # N.B. `hash(T, h)` with `T::Any` is a dynamic call whose `UInt` result is
        # boxed on return (16 boxes per `Signature` on a typical call). `objectid`
        # is a builtin with a statically-known `UInt` return, so
        # `hash(objectid(T), h)` hashes the same identity with no boxing and no
        # dynamic dispatch.
        #
        # This changes the hash *values*, which is safe here because `Signature`
        # hashes never cross a process boundary: they are only ever used as keys
        # in process-local tables -- `state.signature_time_cost` /
        # `state.signature_alloc_cost` (scheduler-local `LockedObject{Dict}`s) and
        # `SIGNATURE_DEFAULT_CACHE` (a `TaskLocalValue`) -- and `Signature`s
        # themselves are only stored in `Thunk.sig`, which is never serialized.
        # Hash full signature
        h = hash(Signature)
        for T in sig
            h = hash(objectid(T), h)
        end

        # Hash non-kwarg signature
        @assert isdefined(Core, :kwcall) "FIXME: No kwcall! Use kwfunc"
        idx = findfirst(T->T===typeof(Core.kwcall), sig)
        if idx !== nothing
            # Skip NT kwargs
            sig_nokw = @view sig[idx+2:end]
        else
            sig_nokw = @view sig[1:end]
        end
        h_nokw = hash(Signature, UInt(1))
        for T in sig_nokw
            # N.B. `objectid` for the same reason as above.
            h_nokw = hash(objectid(T), h_nokw)
        end

        return new(sig, h, sig_nokw, h_nokw)
    end
end
Base.hash(sig::Signature, h::UInt) = hash(sig.hash, h)
Base.isequal(sig1::Signature, sig2::Signature) = sig1.hash == sig2.hash
