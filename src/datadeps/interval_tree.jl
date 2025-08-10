# Get the start address of a span
span_start(span::MemorySpan) = span.ptr.addr
span_start(span::LocalMemorySpan) = span.ptr
span_start(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_start(span.spans[i]), N))
# Get the length of a span
span_len(span::MemorySpan) = span.len
span_len(span::LocalMemorySpan) = span.len
span_len(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_len(span.spans[i]), N))

# Get the end address of a span
span_end(span::MemorySpan) = span.ptr.addr + span.len
span_end(span::LocalMemorySpan) = span.ptr + span.len
span_end(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_end(span.spans[i]), N))
mutable struct IntervalNode{M,E}
    span::M
    max_end::E  # Maximum end value in this subtree
    left::Union{IntervalNode{M,E}, Nothing}
    right::Union{IntervalNode{M,E}, Nothing}

    IntervalNode(span::M) where M <: MemorySpan = new{M,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::LocalMemorySpan) = new{LocalMemorySpan,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::ManyMemorySpan{N}) where N = new{ManyMemorySpan{N},ManyPair{N}}(span, span_end(span), nothing, nothing)
end

mutable struct IntervalTree{M,E}
    root::Union{IntervalNode{M,E}, Nothing}

    IntervalTree{M}() where M<:MemorySpan = new{M,UInt64}(nothing)
    IntervalTree{LocalMemorySpan}() = new{LocalMemorySpan,UInt64}(nothing)
    IntervalTree{ManyMemorySpan{N}}() where N = new{ManyMemorySpan{N},ManyPair{N}}(nothing)
end

# Construct interval tree from unsorted set of spans
function IntervalTree{M}(spans) where M
    tree = IntervalTree{M}()
    for span in spans
        insert!(tree, span)
    end
    return tree
end
IntervalTree(spans::Vector{M}) where M = IntervalTree{M}(spans)

function Base.show(io::IO, tree::IntervalTree)
    println(io, "$(typeof(tree)) (with $(length(tree)) spans):")
    for (i, span) in enumerate(tree)
        println(io, "  $i: [$(span_start(span)), $(span_end(span))) (len=$(span_len(span)))")
    end
end

function Base.collect(tree::IntervalTree{M}) where M
    result = M[]
    for span in tree
        push!(result, span)
    end
    return result
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
    current = popfirst!(state)
    if current.right !== nothing
        pushfirst!(state, current.right)
    end
    if current.left !== nothing
        pushfirst!(state, current.left)
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
    node.max_end = span_end(node.span)
    if node.left !== nothing
        node.max_end = max(node.max_end, node.left.max_end)
    end
    if node.right !== nothing
        node.max_end = max(node.max_end, node.right.max_end)
    end
end

# Insert a span into the interval tree
function Base.insert!(tree::IntervalTree{M}, span::M) where M
    if !isempty(span)
        tree.root = insert_node!(tree.root, span)
    end
    return span
end

function insert_node!(::Nothing, span::M) where M
    return IntervalNode(span)
end
function insert_node!(node::IntervalNode{M,E}, span::M) where {M,E}
    if span_start(span) <= span_start(node.span)
        node.left = insert_node!(node.left, span)
    else
        node.right = insert_node!(node.right, span)
    end

    update_max_end!(node)
    return node
end

# Remove a specific span from the tree (split as needed)
function Base.delete!(tree::IntervalTree{M}, span::M) where M
    if !isempty(span)
        tree.root = delete_node!(tree.root, span)
    end
    return span
end

function delete_node!(::Nothing, span::M) where M
    return nothing
end
function delete_node!(node::IntervalNode{M,E}, span::M) where {M,E}
    # Check for exact match first
    if span_start(node.span) == span_start(span) && span_len(node.span) == span_len(span)
        # Exact match, remove the node
        if node.left === nothing && node.right === nothing
            return nothing
        elseif node.left === nothing
            return node.right
        elseif node.right === nothing
            return node.left
        else
            # Node has two children - replace with inorder successor
            successor = find_min(node.right)
            node.span = successor.span
            node.right = delete_node!(node.right, successor.span)
        end
    # Check for overlap
    elseif spans_overlap(node.span, span)
        # Handle overlapping spans by removing current node and adding remainders
        original_span = node.span

        # Remove the current node first (same logic as exact match)
        if node.left === nothing && node.right === nothing
            # Leaf node - remove it and create a new subtree with remainders
            remaining_node = nothing
        elseif node.left === nothing
            remaining_node = node.right
        elseif node.right === nothing
            remaining_node = node.left
        else
            # Node has two children - replace with inorder successor
            successor = find_min(node.right)
            node.span = successor.span
            node.right = delete_node!(node.right, successor.span)
            remaining_node = node
        end

        # Calculate and insert the remaining portions
        original_start = span_start(original_span)
        original_end = span_end(original_span)
        del_start = span_start(span)
        del_end = span_end(span)

        # Left portion: exists if original starts before deleted span
        if original_start < del_start
            left_end = min(original_end, del_start)
            if left_end > original_start
                left_span = M(original_start, left_end - original_start)
                if !isempty(left_span)
                    remaining_node = insert_node!(remaining_node, left_span)
                end
            end
        end

        # Right portion: exists if original extends beyond deleted span
        if original_end > del_end
            right_start = max(original_start, del_end)
            if original_end > right_start
                right_span = M(right_start, original_end - right_start)
                if !isempty(right_span)
                    remaining_node = insert_node!(remaining_node, right_span)
                end
            end
        end

        return remaining_node
    elseif span_start(span) <= span_start(node.span)
        node.left = delete_node!(node.left, span)
    else
        node.right = delete_node!(node.right, span)
    end

    if node !== nothing
        update_max_end!(node)
    end
    return node
end

function find_min(node::IntervalNode)
    while node.left !== nothing
        node = node.left
    end
    return node
end

# Check if two spans overlap
function spans_overlap(span1::MemorySpan, span2::MemorySpan)
    return span_start(span1) < span_end(span2) && span_start(span2) < span_end(span1)
end
function spans_overlap(span1::LocalMemorySpan, span2::LocalMemorySpan)
    return span_start(span1) < span_end(span2) && span_start(span2) < span_end(span1)
end
function spans_overlap(span1::ManyMemorySpan{N}, span2::ManyMemorySpan{N}) where N
    # N.B. The spans are assumed to be the same length and relative offset
    return spans_overlap(span1.spans[1], span2.spans[1])
end

# Find all spans that overlap with the given query span
function find_overlapping(tree::IntervalTree{M}, query::M) where M
    result = M[]
    find_overlapping!(tree.root, query, result)
    return result
end

function find_overlapping!(::Nothing, query::M, result::Vector{M}) where M
    return
end
function find_overlapping!(node::IntervalNode{M,E}, query::M, result::Vector{M}) where {M,E}
    # Check if current node overlaps with query
    if spans_overlap(node.span, query)
        # Get the overlapping portion of the span
        overlap_start = max(span_start(node.span), span_start(query))
        overlap_end = min(span_end(node.span), span_end(query))
        overlap = M(overlap_start, overlap_end - overlap_start)
        push!(result, overlap)
    end

    # Recursively search left subtree if it might contain overlapping intervals
    if node.left !== nothing && node.left.max_end > span_start(query)
        find_overlapping!(node.left, query, result)
    end

    # Recursively search right subtree if query extends beyond current node's start
    if node.right !== nothing && span_end(query) > span_start(node.span)
        find_overlapping!(node.right, query, result)
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
1. Finds all overlapping spans in the tree
2. Removes each overlapping span
3. Adds back the non-overlapping portions (left and/or right remnants)
4. If diff is provided, add the overlapping span to diff
"""
function subtract_single_span!(tree::IntervalTree{M}, sub_span::M, diff=nothing) where M
    # Find all spans that overlap with the subtrahend
    overlapping_spans = find_overlapping(tree, sub_span)

    # Process each overlapping span
    for overlap_span in overlapping_spans
        # Remove the overlapping span from the tree
        delete!(tree, overlap_span)

        # Calculate and add back the portions that should remain
        add_remaining_portions!(tree, overlap_span, sub_span)

        if diff !== nothing && !isempty(overlap_span)
            push!(diff, overlap_span)
        end
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
        left_end = min(original_end, sub_start)
        if left_end > original_start
            left_span = M(original_start, left_end - original_start)
            if !isempty(left_span)
                insert!(tree, left_span)
            end
        end
    end

    # Right portion: exists if original extends beyond subtracted
    if original_end > sub_end
        right_start = max(original_start, sub_end)
        if original_end > right_start
            right_span = M(right_start, original_end - right_start)
            if !isempty(right_span)
                insert!(tree, right_span)
            end
        end
    end
end