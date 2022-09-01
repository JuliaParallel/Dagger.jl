# unified hash algorithm

using Dagger

uhash(x, h::UInt)::UInt = hash(x, h)
function uhash(x::Dagger.Thunk, h::UInt; sig=nothing)::UInt
    value = hash(0xdead7453, h)
    if x.hash != UInt(0)
        return uhash(x.hash, value)
    end
    @assert sig !== nothing 
    tt = Any[]
    for input in x.inputs
        input = unwrap_weak_checked(input)
        if input isa Dagger.Thunk && input.hash != UInt(0)
            value = uhash(input.hash, value)
        else
            value = uhash(input, value)
            push!(tt, typeof(input))
        end
    end
    sig = (typeof(x.f), tt)
    value = uhash_sig(sig, value)
    x.hash = value
    return value
end
uhash(x::Dagger.WeakThunk, h::UInt)::UInt =
    uhash(Dagger.unwrap_weak_checked(x), h)
function uhash_sig((f, tt), h::UInt)::UInt
    value = hash(0xdead5160, h)
    ci_list = Base.code_typed(f, tt)
    if length(ci_list) == 0
        return hash(Union{}, hash(typeof(f), hash(tt, value)))
    end
    # tt must be concrete
    ci = first(only(ci_list))::Core.CodeInfo
    return uhash_code(ci, hash(typeof(f), hash(tt, value)))
end
function uhash_code(ci::Core.CodeInfo, h::UInt)::UInt
    value = hash(0xdeadc0de, h)
    for insn in ci.code
        dump(insn)
        value = uhash_insn(insn, h)
    end
    return value
end
function uhash_insn(insn::Expr, h::UInt)::UInt
    value = hash(0xdeadeec54, h)
    value = hash(insn.head, value)
    for arg in insn.args
        dump(insn)
        @show uhash_insn(arg, value)
        value = uhash_insn(arg, value)
    end
    return value
end
function uhash_insn(insn::GlobalRef, h::UInt)::UInt
    value = hash(0xdead6147, h)
    return hash(nameof(insn.mod), hash(insn.name, value))
end
uhash_insn(insn, h::UInt)::UInt = hash(insn, h)
