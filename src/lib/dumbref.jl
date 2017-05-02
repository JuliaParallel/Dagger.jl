# manually-released RemoteRef alternative
immutable MemToken
    where::Int
    key::Int
    size::Int # size in bytes
    released::Bool # has this been released but asked to be cached
end

const MAX_MEMORY = Ref{Float64}((Sys.total_memory() / nprocs()) / 2) # half the process's share
const _mymem = Dict{Int,Any}()
const _token_order = MemToken[]

let token_count = 0
    global next_token_id
    next_token_id() = (token_count+=1)
end

function data_size(d)
    Base.summarysize(d)
end

function data_size(xs::AbstractArray{String})
    # doesn't check for redundant references, but
    # really super fast in comparison to summarysize
    sum(map(sizeof, xs))
end

function make_token(data)
    sz = data_size(data)
    tok = MemToken(myid(), next_token_id(), sz, false)
    total_size = sum(map(x->x.size, _token_order)) + sz

    i = 1
    while total_size > MAX_MEMORY[] && i <= length(_token_order)
        # we need to weed out some old data here
        # if everything that can be has been cleaned up
        t = _token_order[i]
        if t.released
            filter!(x->x.key != t.key, _token_order)
            x = pop!(_mymem, t.key)
            total_size -= t.size
            @logmsg("cached & released $t - $(t.size)B dropped")
        end
        i += 1
    end
    push!(_token_order, tok)
    _mymem[tok.key] = data
    tok
end

function release_token(tok, keeparound=false)
    if tok.where == myid()
        if keeparound
            # set released to true, but don't remove it yet.
            tok_released = MemToken(tok.where, tok.key, tok.size, true)
            idx = find(x->x.key == tok.key, _token_order)
            _token_order[idx] = tok_released
            @logmsg("soft-released $tok - $(tok.size)B freed")
        else
            filter!(x->x.key != tok.key, _token_order)
            x = pop!(_mymem, tok.key)
            @logmsg("removed $tok - $(tok.size)B freed")
        end
    else
        remotecall_fetch(()->release_token(tok, keeparound), tok.where)
    end
    nothing
end

function Base.fetch(t::MemToken)
    if t.where == myid()
        if haskey(_mymem, t.key)
            return Nullable{Any}(_mymem[t.key])
        else
            return Nullable{Any}()
        end
    else
        remotecall_fetch(()->fetch(t), t.where)
    end
end

function unrelease_token(tok)
    # first we need to check if the token was removed due cache pruning

    if tok.where == myid()
        # set released to true, but don't remove it yet.
        if !haskey(_mymem, tok.key)
            return false
        end
        tok_unreleased = MemToken(tok.where, tok.key, tok.size, false)
        # keep LRU order
        idx = find(x->x.key == tok.key, _token_order) |> first
        # copy everything after this token one step left
        l = length(_token_order)
        if idx != l
            _token_order[idx:l-1] = view(_token_order, (idx+1:l))
            # set the last token to the unreleased token
            _token_order[end] = tok_unreleased
        end
        true
    else
        remotecall_fetch(()->unrelease_token(tok), tok.where)
    end
end

