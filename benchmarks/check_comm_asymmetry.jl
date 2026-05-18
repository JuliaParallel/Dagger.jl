#!/usr/bin/env julia
# Parse MPI+Dagger logs and report communication decision asymmetry per tag.
# Asymmetry: for the same tag, one rank decides to send (local+bcast, sender+communicated, etc.)
# and another rank decides to infer (inferred, uninvolved) and never recv → deadlock.
#
# Usage: julia check_comm_asymmetry.jl < logfile
# Or: mpiexec -n 10 julia ... run_matmul.jl 2>&1 | tee matmul.log; julia check_comm_asymmetry.jl < matmul.log

const SEND_DECISIONS = Set([
    "local+bcast", "sender+communicated", "sender+inferred", "receiver+bcast",
    "aliasing",  # when followed by local+bcast we already capture local+bcast
])
const RECV_DECISIONS = Set([
    "communicated", "receiver", "sender+communicated",  # received data
])
const INFER_DECISIONS = Set([
    "inferred", "uninvolved",  # did not recv (uses inferred type)
])

function parse_line(line)
    # Match [rank X][tag Y] then any [...] and capture the last bracket pair before space or end
    rank = nothing
    tag = nothing
    decision = nothing
    category = nothing  # aliasing, execute!, remotecall_endpoint
    for m in eachmatch(r"\[rank\s+(\d+)\]", line)
        rank = parse(Int, m.captures[1])
    end
    for m in eachmatch(r"\[tag\s+(\d+)\]", line)
        tag = parse(Int, m.captures[1])
    end
    for m in eachmatch(r"\[(execute!|aliasing|remotecall_endpoint)\]", line)
        category = m.captures[1]
    end
    # Decision is usually in last [...] that looks like [word] or [word+word]
    for m in eachmatch(r"\]\[([^\]]+)\]", line)
        candidate = m.captures[1]
        # Normalize: "communicated" "inferred" "local+bcast" "sender+inferred" "receiver" etc.
        if occursin("inferred", candidate) && !occursin("communicated", candidate)
            decision = "inferred"
            break
        elseif occursin("communicated", candidate)
            decision = "communicated"
            break
        elseif occursin("local+bcast", candidate)
            decision = "local+bcast"
            break
        elseif occursin("sender+", candidate)
            decision = startswith(candidate, "sender+inferred") ? "sender+inferred" : "sender+communicated"
            break
        elseif candidate == "receiver"
            decision = "receiver"
            break
        elseif candidate == "receiver+bcast"
            decision = "receiver+bcast"
            break
        elseif candidate == "inplace_move"
            decision = "inplace_move"
            break
        end
    end
    return rank, tag, category, decision
end

function main()
    # tag => Dict(rank => decision)
    by_tag = Dict{Int, Dict{Int, String}}()
    for line in eachline(stdin)
        rank, tag, category, decision = parse_line(line)
        isnothing(rank) && continue
        isnothing(tag) && continue
        isnothing(decision) && continue
        if !haskey(by_tag, tag)
            by_tag[tag] = Dict{Int, String}()
        end
        by_tag[tag][rank] = decision
    end

    # For each tag, check: is there at least one sender and one inferrer (non-receiver)?
    send_keys = Set(["local+bcast", "sender+communicated", "sender+inferred", "receiver+bcast"])
    infer_keys = Set(["inferred", "sender+inferred"])  # sender+inferred means sender didn't need to recv
    recv_keys = Set(["communicated", "receiver", "sender+communicated"])

    asymmetries = []
    for (tag, ranks) in sort(collect(by_tag), by = first)
        senders = [r for (r, d) in ranks if d in send_keys]
        inferrers = [r for (r, d) in ranks if d in infer_keys || d == "uninvolved"]
        receivers = [r for (r, d) in ranks if d in recv_keys]
        # Asymmetry: someone sends (bcast) so will send to ALL other ranks; someone chose infer and won't recv.
        if !isempty(senders) && !isempty(inferrers)
            push!(asymmetries, (tag, senders, inferrers, receivers, ranks))
        end
    end

    if isempty(asymmetries)
        println("No communication decision asymmetry found (no tag has both sender and inferrer).")
        return
    end

    println("=== Communication decision asymmetry (can cause deadlock) ===\n")
    for (tag, senders, inferrers, receivers, ranks) in asymmetries
        println("Tag $tag:")
        println("  Senders (will bcast to all others): $senders")
        println("  Inferrers (did not recv): $inferrers")
        println("  Receivers: $receivers")
        println("  All decisions: $ranks")
        println()
    end
end

main()
