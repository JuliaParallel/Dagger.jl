mutable struct IntervalNode{M,E}
    span::M
    max_end::E  # Maximum end value in this subtree
    left::Union{IntervalNode{M,E}, Nothing}
    right::Union{IntervalNode{M,E}, Nothing}

    IntervalNode(span::M) where M <: MemorySpan = new{M,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::LocalMemorySpan) = new{LocalMemorySpan,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::ManyMemorySpan{N}) where N = new{ManyMemorySpan{N},ManyPair{N}}(span, span_end(span), nothing, nothing)
    IntervalNode(span::LocatorMemorySpan{T}) where T = new{LocatorMemorySpan{T},UInt64}(span, span_end(span), nothing, nothing)
end

mutable struct IntervalTree{M,E}
    root::Union{IntervalNode{M,E}, Nothing}

    IntervalTree{M}() where M<:MemorySpan = new{M,UInt64}(nothing)
    IntervalTree{LocalMemorySpan}() = new{LocalMemorySpan,UInt64}(nothing)
    IntervalTree{ManyMemorySpan{N}}() where N = new{ManyMemorySpan{N},ManyPair{N}}(nothing)
    IntervalTree{LocatorMemorySpan{T}}() where T = new{LocatorMemorySpan{T},UInt64}(nothing)
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
    max_end = span_end(node.span)
    if node.left !== nothing
        max_end = max(max_end, node.left.max_end)
    end
    if node.right !== nothing
        max_end = max(max_end, node.right.max_end)
    end
    node.max_end = max_end
end

# Insert a span into the interval tree
function Base.insert!(tree::IntervalTree{M,E}, span::M) where {M,E}
    if !isempty(span)
        if tree.root === nothing
            tree.root = IntervalNode(span)
            update_max_end!(tree.root)
            return span
        end
        #tree.root = insert_node!(tree.root, span)
        to_update = Vector{IntervalNode{M,E}}()
        prev_node = tree.root
        cur_node = tree.root
        while cur_node !== nothing
            if span_start(span) <= span_start(cur_node.span)
                cur_node = cur_node.left
            else
                cur_node = cur_node.right
            end
            if cur_node !== nothing
                prev_node = cur_node
                push!(to_update, cur_node)
            end
        end
        if prev_node.left === nothing
            prev_node.left = IntervalNode(span)
        else
            prev_node.right = IntervalNode(span)
        end
        for node_idx in eachindex(to_update)
            node = to_update[node_idx]
            update_max_end!(node)
        end
    end
    return span
end

function insert_node!(::Nothing, span::M) where M
    return IntervalNode(span)
end
function insert_node!(root::IntervalNode{M,E}, span::M) where {M,E}
    # Use a queue to track the path for updating max_end after insertion
    path = Vector{IntervalNode{M,E}}()
    current = root

    # Traverse to find the insertion point
    while current !== nothing
        push!(path, current)
        if span_start(span) <= span_start(current.span)
            if current.left === nothing
                current.left = IntervalNode(span)
                break
            end
            current = current.left
        else
            if current.right === nothing
                current.right = IntervalNode(span)
                break
            end
            current = current.right
        end
    end

    # Update max_end for all ancestors (process in reverse order)
    while !isempty(path)
        node = pop!(path)
        update_max_end!(node)
    end

    return root
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
function delete_node!(root::IntervalNode{M,E}, span::M) where {M,E}
    # Track the path to the target node: (node, direction_to_child)
    path = Vector{Tuple{IntervalNode{M,E}, Symbol}}()
    current = root
    target = nothing
    target_type = :none  # :exact or :overlap

    # Phase 1: Search for target node
    while current !== nothing
        is_exact = span_start(current.span) == span_start(span) && span_len(current.span) == span_len(span)
        is_overlap = !is_exact && spans_overlap(current.span, span)

        if is_exact
            target = current
            target_type = :exact
            break
        elseif is_overlap
            target = current
            target_type = :overlap
            break
        elseif span_start(span) <= span_start(current.span)
            push!(path, (current, :left))
            current = current.left
        else
            push!(path, (current, :right))
            current = current.right
        end
    end

    if target === nothing
        return root
    end

    # Phase 2: Compute replacement for target node
    original_span = target.span
    succ_path = Vector{IntervalNode{M,E}}()  # Path to successor (for max_end updates)
    local replacement::Union{IntervalNode{M,E}, Nothing}

    if target.left === nothing && target.right === nothing
        # Leaf node
        replacement = nothing
    elseif target.left === nothing
        # Only right child
        replacement = target.right
    elseif target.right === nothing
        # Only left child
        replacement = target.left
    else
        # Two children - find and remove inorder successor
        successor = find_min(target.right)

        if target.right === successor
            # Successor is direct right child
            target.right = successor.right
        else
            # Track path to successor for max_end updates
            succ_parent = target.right
            push!(succ_path, succ_parent)
            while succ_parent.left !== successor
                succ_parent = succ_parent.left
                push!(succ_path, succ_parent)
            end
            # Remove successor by replacing with its right child
            succ_parent.left = successor.right
        end

        target.span = successor.span
        replacement = target
    end

    # Phase 3: Handle overlap case - add remaining portions
    if target_type == :overlap
        original_start = span_start(original_span)
        original_end = span_end(original_span)
        del_start = span_start(span)
        del_end = span_end(span)
        verify_span(span)

        # Left portion: exists if original starts before deleted span
        if original_start < del_start
            left_end = min(original_end, del_start - _span_one(del_start))
            if left_end >= original_start
                left_span = M(original_start, left_end - original_start + _span_one(left_end))
                if !isempty(left_span)
                    replacement = insert_node!(replacement, left_span)
                end
            end
        end

        # Right portion: exists if original extends beyond deleted span
        if original_end > del_end
            right_start = max(original_start, del_end + _span_one(del_end))
            if original_end >= right_start
                right_span = M(right_start, original_end - right_start + _span_one(original_end))
                if !isempty(right_span)
                    replacement = insert_node!(replacement, right_span)
                end
            end
        end
    end

    # Phase 4: Update parent's child pointer
    if isempty(path)
        root = replacement
    else
        parent, dir = path[end]
        if dir == :left
            parent.left = replacement
        else
            parent.right = replacement
        end
    end

    # Phase 5: Update max_end in correct order (bottom-up)
    # First: successor path (if any)
    for i in length(succ_path):-1:1
        update_max_end!(succ_path[i])
    end
    # Second: target node (if it wasn't removed)
    if replacement === target
        update_max_end!(target)
    end
    # Third: main path (ancestors of target)
    for i in length(path):-1:1
        update_max_end!(path[i][1])
    end

    return root
end

function find_min(node::IntervalNode)
    while node.left !== nothing
        node = node.left
    end
    return node
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
    # Use a queue for breadth-first traversal
    queue = Vector{IntervalNode{M,E}}()
    push!(queue, node)

    while !isempty(queue)
        current = popfirst!(queue)

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

        # Enqueue left subtree if it might contain overlapping intervals
        if current.left !== nothing && current.left.max_end >= span_start(query)
            push!(queue, current.left)
        end

        # Enqueue right subtree if query extends beyond current node's start
        if current.right !== nothing && span_end(query) >= span_start(current.span)
            push!(queue, current.right)
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
