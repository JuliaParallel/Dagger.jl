# A treap: a binary search tree on `span_start` that is kept balanced by random node priorities
# (a node's priority is never smaller than its children's). Memory spans arrive in address order
# very often (arrays and tiles allocated one after another), and an unbalanced tree then degrades to a
# linked list: inserting 16000 increasing spans cost 34 us each against 0.1 us for the same spans
# shuffled, and a Datadeps region over many tiles spent 40-85 % of its planning time in `insert!`.
#
# The priorities come from a per-tree counter, not from the span: the shape of a tree must be the same on
# every MPI rank that performs the same inserts (the order in which `iterate`/`find_overlapping!` return
# spans follows the shape, and ranks must plan identically), and spans carry rank-local addresses.
# Equal starts may therefore sit on either side of each other after a rotation; only `delete!` searched for
# an exact span and it looks on both sides for ties.
mutable struct IntervalNode{M,E}
    span::M
    max_end::E  # Maximum end value in this subtree
    prio::UInt64
    left::Union{IntervalNode{M,E}, Nothing}
    right::Union{IntervalNode{M,E}, Nothing}

    IntervalNode(span::M) where M <: MemorySpan = new{M,UInt64}(span, span_end(span), UInt64(0), nothing, nothing)
    IntervalNode(span::LocalMemorySpan) = new{LocalMemorySpan,UInt64}(span, span_end(span), UInt64(0), nothing, nothing)
    IntervalNode(span::ManyMemorySpan{N}) where N = new{ManyMemorySpan{N},ManyPair{N}}(span, span_end(span), UInt64(0), nothing, nothing)
    IntervalNode(span::LocatorMemorySpan{T}) where T = new{LocatorMemorySpan{T},UInt64}(span, span_end(span), UInt64(0), nothing, nothing)
end

mutable struct IntervalTree{M,E}
    root::Union{IntervalNode{M,E}, Nothing}
    seq::UInt64   # number of nodes created so far: the source of node priorities

    IntervalTree{M}() where M<:MemorySpan = new{M,UInt64}(nothing, UInt64(0))
    IntervalTree{LocalMemorySpan}() = new{LocalMemorySpan,UInt64}(nothing, UInt64(0))
    IntervalTree{ManyMemorySpan{N}}() where N = new{ManyMemorySpan{N},ManyPair{N}}(nothing, UInt64(0))
    IntervalTree{LocatorMemorySpan{T}}() where T = new{LocatorMemorySpan{T},UInt64}(nothing, UInt64(0))
end

# splitmix64: a deterministic, well-mixed priority for the n-th node of a tree
@inline function _node_priority(n::UInt64)
    z = n + 0x9e3779b97f4a7c15
    z = (z ⊻ (z >> 30)) * 0xbf58476d1ce4e5b9
    z = (z ⊻ (z >> 27)) * 0x94d049bb133111eb
    return z ⊻ (z >> 31)
end

# Construct interval tree from unsorted set of spans
function IntervalTree{M}(spans) where M
    tree = IntervalTree{M}()
    for span in spans
        insert!(tree, span)
    end
    verify_spans(tree)
    return tree
end
IntervalTree(spans::Vector{M}) where M = IntervalTree{M}(spans)

_span_one(x::Unsigned) = one(typeof(x))
_span_one(x::ManyPair{N}) where N = ManyPair(ntuple(_ -> UInt(1), N))

function Base.show(io::IO, tree::IntervalTree)
    println(io, "$(typeof(tree)) (with $(length(tree)) spans):")
    for (i, span) in enumerate(tree)
        println(io, "  $i: [$(span_start(span)), $(span_end(span))] (len=$(span_len(span)))")
    end
end

function Base.collect(tree::IntervalTree{M}) where M
    result = M[]
    for span in tree
        push!(result, span)
    end
    return result
end

# Useful for debugging when spans get misaligned
function verify_spans(tree::IntervalTree{ManyMemorySpan{N}}) where N
    for span in tree
        verify_span(span)
    end
end

function Base.iterate(tree::IntervalTree{M}) where M
    state = Vector{M}()
    if tree.root === nothing
        return nothing
    end
    return iterate(tree.root)
end
function Base.iterate(tree::IntervalTree, state)
    return iterate(tree.root, state)
end
function Base.iterate(root::IntervalNode{M,E}) where {M,E}
    state = Vector{IntervalNode{M,E}}()
    push!(state, root)
    return iterate(root, state)
end
function Base.iterate(root::IntervalNode, state)
    if isempty(state)
        return nothing
    end
    current = pop!(state)
    if current.right !== nothing
        push!(state, current.right)
    end
    if current.left !== nothing
        push!(state, current.left)
    end
    return current.span, state
end

function Base.length(tree::IntervalTree)
    result = 0
    for _ in tree
        result += 1
    end
    return result
end

# Update max_end value for a node based on its children
function update_max_end!(node::IntervalNode)
    max_end = span_end(node.span)
    if node.left !== nothing
        max_end = max(max_end, node.left.max_end)
    end
    if node.right !== nothing
        max_end = max(max_end, node.right.max_end)
    end
    node.max_end = max_end
end

# Rotations that keep the search order and restore `max_end` of the two nodes whose children changed
function _rotate_right!(node::IntervalNode{M,E}) where {M,E}
    l = node.left::IntervalNode{M,E}
    node.left = l.right
    l.right = node
    update_max_end!(node)
    update_max_end!(l)
    return l
end
function _rotate_left!(node::IntervalNode{M,E}) where {M,E}
    r = node.right::IntervalNode{M,E}
    node.right = r.left
    r.left = node
    update_max_end!(node)
    update_max_end!(r)
    return r
end

# Insert `new` below `node` and return the root of that subtree; expected depth is O(log n)
function _treap_insert!(node::Union{IntervalNode{M,E},Nothing}, new::IntervalNode{M,E}) where {M,E}
    node === nothing && return new
    if span_start(new.span) <= span_start(node.span)
        node.left = _treap_insert!(node.left, new)
        if (node.left::IntervalNode{M,E}).prio > node.prio
            return _rotate_right!(node)
        end
    else
        node.right = _treap_insert!(node.right, new)
        if (node.right::IntervalNode{M,E}).prio > node.prio
            return _rotate_left!(node)
        end
    end
    update_max_end!(node)
    return node
end

# Insert a span into the interval tree
function Base.insert!(tree::IntervalTree{M,E}, span::M) where {M,E}
    if isempty(span)
        return span
    end
    node = IntervalNode(span)
    node.prio = _node_priority(tree.seq)
    tree.seq += 1
    tree.root = _treap_insert!(tree.root, node)
    return span
end

# The node holding exactly `span`, with its ancestors pushed on `path` (left empty when there is none).
# Equal starts can be on either side of a node (rotations), so a tie is searched on both.
function _find_exact!(path::Vector{IntervalNode{M,E}}, node::Union{IntervalNode{M,E},Nothing}, span::M) where {M,E}
    node === nothing && return nothing
    push!(path, node)
    ns = span_start(node.span)
    s = span_start(span)
    if ns == s && span_len(node.span) == span_len(span)
        return node
    end
    found = nothing
    if s <= ns
        found = _find_exact!(path, node.left, span)
    end
    if found === nothing && s >= ns
        found = _find_exact!(path, node.right, span)
    end
    found === nothing && pop!(path)
    return found
end

# Remove a specific span from the tree (split as needed)
function Base.delete!(tree::IntervalTree{M,E}, span::M) where {M,E}
    if isempty(span) || tree.root === nothing
        return span
    end

    # Search for target node, keeping track of path
    path = Vector{IntervalNode{M,E}}()
    target = _find_exact!(path, tree.root, span)

    if target === nothing
        return span
    end

    # Standard BST deletion
    if target.left !== nothing && target.right !== nothing
        # Two children: replace target's span with inorder successor's span, then delete successor
        succ_path = Vector{IntervalNode{M,E}}()
        succ = target.right
        while succ.left !== nothing
            push!(succ_path, succ)
            succ = succ.left
        end

        target.span = succ.span

        # Remove successor node (it has at most one child: right)
        parent_of_succ = isempty(succ_path) ? target : succ_path[end]
        replacement = succ.right
        if parent_of_succ.left === succ
            parent_of_succ.left = replacement
        else
            parent_of_succ.right = replacement
        end

        # Update max_end bottom-up for the successor's original path
        update_max_end!(parent_of_succ)
        for i in length(succ_path)-1:-1:1
            update_max_end!(succ_path[i])
        end
    else
        # Zero or one child
        replacement = target.left !== nothing ? target.left : target.right
        pop!(path) # Remove target from path since it's being removed from the tree
        if isempty(path)
            tree.root = replacement
        else
            parent = path[end]
            if parent.left === target
                parent.left = replacement
            else
                parent.right = replacement
            end
        end
    end

    # Update max_end for the main path (ancestors of the removed/modified node)
    for i in length(path):-1:1
        update_max_end!(path[i])
    end

    return span
end

# Find all spans that overlap with the given query span
function find_overlapping(tree::IntervalTree{M}, query::M; exact::Bool=true) where M
    result = M[]
    find_overlapping!(tree.root, query, result; exact)
    return result
end
function find_overlapping!(tree::IntervalTree{M}, query::M, result::Vector{M}; exact::Bool=true) where M
    find_overlapping!(tree.root, query, result; exact)
    return result
end

function find_overlapping!(::Nothing, query::M, result::Vector{M}; exact::Bool=true) where M
    return
end
function find_overlapping!(node::IntervalNode{M,E}, query::M, result::Vector{M}; exact::Bool=true) where {M,E}
    # Use a stack for iterative depth-first traversal
    stack = Vector{IntervalNode{M,E}}()
    push!(stack, node)

    while !isempty(stack)
        current = pop!(stack)

        # Check if current node overlaps with query
        if spans_overlap(current.span, query)
            if exact
                # Get the overlapping portion of the span
                overlap = span_diff(current.span, query)
                if !isempty(overlap)
                    push!(result, overlap)
                end
            else
                push!(result, current.span)
            end
        end

        # Search left subtree if its max_end is at least query_start
        if current.left !== nothing && current.left.max_end >= span_start(query)
            push!(stack, current.left)
        end

        # Search right subtree if it could contain an overlap
        if current.right !== nothing &&
           span_start(current.span) <= span_end(query) &&
           current.right.max_end >= span_start(query)
            push!(stack, current.right)
        end
    end
end

# ============================================================================
# MAIN SUBTRACTION ALGORITHM
# ============================================================================

"""
    subtract_spans!(minuend_tree::IntervalTree{M}, subtrahend_spans::Vector{M}, diff=nothing) where M

Subtract all spans in subtrahend_spans from the minuend_tree in-place.
The minuend_tree is modified to contain only the portions that remain after subtraction.

Time Complexity: O(M log N + M*K) where M = |subtrahend_spans|, N = |minuend nodes|, 
                 K = average overlaps per subtrahend span
Space Complexity: O(1) additional space (modifies tree in-place)

If `diff` is provided, add the overlapping spans to `diff`.
"""
function subtract_spans!(minuend_tree::IntervalTree{M}, subtrahend_spans::Vector{M}, diff=nothing) where M
    for sub_span in subtrahend_spans
        subtract_single_span!(minuend_tree, sub_span, diff)
    end
end

"""
    subtract_single_span!(tree::IntervalTree, sub_span::MemorySpan, diff=nothing)

Subtract a single span from the interval tree. This function:
1. Finds all intervals in the tree that overlap with the subtrahend
2. Removes each overlapping interval
3. Adds back the non-overlapping portions (left and/or right remnants)
4. If diff is provided, add the overlapping portion to diff
"""
function subtract_single_span!(tree::IntervalTree{M}, sub_span::M, diff=nothing) where M
    # Find all intervals currently in the tree that overlap with the subtrahend
    overlapping_intervals = find_overlapping(tree, sub_span; exact=false)

    # Process each overlapping interval
    for interval in overlapping_intervals
        # If we are tracking the difference, calculate the intersection before deletion
        if diff !== nothing
            overlap = span_diff(interval, sub_span)
            if !isempty(overlap)
                push!(diff, overlap)
            end
        end

        # Remove the original interval from the tree
        delete!(tree, interval)

        # Calculate and add back the portions that should remain
        add_remaining_portions!(tree, interval, sub_span)
    end
end

"""
    add_remaining_portions!(tree::IntervalTree, original::MemorySpan, subtracted::MemorySpan)

After removing an overlapping span, add back the portions that don't overlap with the subtracted span.
There can be up to two remaining portions: left and right of the subtracted region.
"""
function add_remaining_portions!(tree::IntervalTree{M}, original::M, subtracted::M) where M
    original_start = span_start(original)
    original_end = span_end(original)
    sub_start = span_start(subtracted)
    sub_end = span_end(subtracted)

    # Left portion: exists if original starts before subtracted
    if original_start < sub_start
        left_end = min(original_end, sub_start - _span_one(sub_start))
        if left_end >= original_start
            left_span = M(original_start, left_end - original_start + _span_one(left_end))
            if !isempty(left_span)
                insert!(tree, left_span)
            end
        end
    end

    # Right portion: exists if original extends beyond subtracted
    if original_end > sub_end
        right_start = max(original_start, sub_end + _span_one(sub_end))
        if original_end >= right_start
            right_span = M(right_start, original_end - right_start + _span_one(original_end))
            if !isempty(right_span)
                insert!(tree, right_span)
            end
        end
    end
end
