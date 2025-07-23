struct Signature
    sig::Vector{Any}#DataType}
    hash::UInt
    sig_nokw::SubArray{Any,1,Vector{Any},Tuple{UnitRange{Int}},true}
    hash_nokw::UInt
    function Signature(sig::Vector{Any})#DataType})
        # Hash full signature
        h = hash(Signature)
        for T in sig
            h = hash(T, h)
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
            h_nokw = hash(T, h_nokw)
        end

        return new(sig, h, sig_nokw, h_nokw)
    end
end
Base.hash(sig::Signature, h::UInt) = hash(sig.hash, h)
Base.isequal(sig1::Signature, sig2::Signature) = sig1.hash == sig2.hash
