const Size = Int

# manually-released RemoteRef alternative
immutable MemToken
    where::Int
    key::Int
    size::Size # size in bytes
end

const MAX_MEMORY = Ref{Float64}((Sys.total_memory() / nprocs()) / 2) # half the process's share
const _mymem = Dict{Int,Tuple{Size, Any}}()
const freeable_lru = MemToken[]

let token_count = 0
    global next_token_id
    next_token_id() = (token_count+=1)
end

function approx_size(d)
    Base.summarysize(d) # note: this is accurate but expensive
end

function approx_size{T}(d::Array{T})
    if isbits(eltype(T))
        sizeof(d)
    else
        Base.summarysize(d)
    end
end

function approx_size(xs::AbstractArray{String})
    # doesn't check for redundant references, but
    # really super fast in comparison to summarysize
    sum(map(sizeof, xs))
end

function make_token(data)
    sz = approx_size(data)
    tok = MemToken(myid(), next_token_id(), sz)

    if !isempty(freeable_lru)
        # take this opportunity to purge cached data if necessary
        total_size = sum(map(first, values(_mymem))) + sz
        deleted = Int[]
        i = 1
        while total_size > MAX_MEMORY[] && i <= length(freeable_lru)
            # we need to weed out some old data here
            # if everything that can be has been cleaned up
            t = freeable_lru[i]
            push!(deleted, i)
            pop!(_mymem, t.key)
            total_size -= t.size
            @logmsg("cached & released $t - $(t.size)B dropped")
            i += 1
        end
        if !isempty(deleted)
            deleteat!(freeable_lru, deleted)
        end
    end

    _mymem[tok.key] = (sz,data)
    tok
end

function release_token(tok, keeparound=false)
    if tok.where == myid()
        if keeparound
            # move token to the lru cache
            # XXX: this doesn't check for duplicates
            # duplicates are removed in unrelease_token
            push!(freeable_lru, tok)
            @logmsg("soft-released $tok - $(tok.size)B freed")
        else
            # XXX: this doesn't check to see if it needs to be removed
            # from freeable_lru - however the data is released.
            pop!(_mymem, tok.key)
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
            return Nullable{Any}(last(_mymem[t.key]))
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
        idx = find(x->x.key == tok.key, freeable_lru)
        deleteat!(freeable_lru, idx)
        true
    else
        remotecall_fetch(()->unrelease_token(tok), tok.where)
    end
end

