# manually-released RemoteRef alternative
immutable MemToken
    where::Int
    key::Int
end

global _mymem = Dict{MemToken,Any}()
let token_count = 0
    global next_token_id
    next_token_id() = (token_count+=1)
end

function make_token(data)
    tok = MemToken(myid(), next_token_id())
    _mymem[tok] = data
    tok
end

function release_token(tok)
    if tok.where == myid()
        x = pop!(_mymem, tok)
        release_blob(x)
        @logmsg("removed $tok - $(sizeof(x))B freed")
    else
        remotecall_fetch(()->release_token(tok), tok.where)
    end
    nothing
end

release_blob(x) = nothing

function Base.fetch(t::MemToken)
    if t.where == myid()
        fetch_again(_mymem[t])
    else
        remotecall_fetch(()->fetch(t), t.where)
    end
end

fetch_again(x) = x
