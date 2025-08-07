# Get the start address of a span
span_start(span::MemorySpan) = span.ptr.addr
span_start(span::LocalMemorySpan) = span.ptr
span_start(span::CopyMemorySpan) = CopyPair(span_start(span.src), span_start(span.dest))
span_start(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_start(span.spans[i]), N))
# Get the length of a span
span_len(span::MemorySpan) = span.len
span_len(span::LocalMemorySpan) = span.len
span_len(span::CopyMemorySpan) = CopyPair(span_len(span.src), span_len(span.dest))
span_len(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_len(span.spans[i]), N))

# Get the end address of a span
span_end(span::MemorySpan) = span.ptr.addr + span.len
span_end(span::LocalMemorySpan) = span.ptr + span.len
span_end(span::CopyMemorySpan) = CopyPair(span_end(span.src), span_end(span.dest))
span_end(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_end(span.spans[i]), N))
mutable struct IntervalNode{M,E}
    span::M
    max_end::E  # Maximum end value in this subtree
    left::Union{IntervalNode{M,E}, Nothing}
    right::Union{IntervalNode{M,E}, Nothing}

    IntervalNode(span::M) where M <: MemorySpan = new{M,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::LocalMemorySpan) = new{LocalMemorySpan,UInt64}(span, span_end(span), nothing, nothing)
    IntervalNode(span::CopyMemorySpan) = new{CopyMemorySpan,CopyPair}(span, span_end(span), nothing, nothing)
    IntervalNode(span::ManyMemorySpan{N}) where N = new{ManyMemorySpan{N},ManyPair{N}}(span, span_end(span), nothing, nothing)
end

mutable struct IntervalTree{M,E}
    root::Union{IntervalNode{M,E}, Nothing}

    IntervalTree{M}() where M<:MemorySpan = new{M,UInt64}(nothing)
    IntervalTree{LocalMemorySpan}() = new{LocalMemorySpan,UInt64}(nothing)
    IntervalTree{CopyMemorySpan}() = new{CopyMemorySpan,CopyPair}(nothing)
    IntervalTree{ManyMemorySpan{N}}() where N = new{ManyMemorySpan{N},ManyPair{N}}(nothing)
end

# Construct interval tree from unsorted set of spans
function IntervalTree{M}(spans) where M
    tree = IntervalTree{M}()
    for span in spans
        Base.insert!(tree, span)
    end
    return tree
end
IntervalTree(spans::Vector{M}) where M = IntervalTree{M}(spans)

# Convert all spans in the tree back to a vector
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
    tree.root = insert_node!(tree.root, span)
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

# Remove a specific span from the tree (exact match)
function Base.delete!(tree::IntervalTree{M}, span::M) where M
    tree.root = delete_node!(tree.root, span)
end

function delete_node!(::Nothing, span::M) where M
    return nothing
end
function delete_node!(node::IntervalNode{M,E}, span::M) where {M,E}
    if span_start(node.span) == span_start(span) && span_len(node.span) == span_len(span)
        # Found the node to remove
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
function spans_overlap(span1::CopyMemorySpan, span2::CopyMemorySpan)
    # N.B. src and dest are assumed to be the same length and relative offset
    return spans_overlap(span1.src, span2.src)
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
        push!(result, node.span)
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

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

"""
    print_tree_spans(tree::IntervalTree)

Helper function to print all spans in the tree (for debugging).
"""
function print_tree_spans(tree::IntervalTree)
    println("Tree contains $(length(tree)) spans:")
    for (i, span) in enumerate(tree)
        println("  $i: [$(span_start(span)), $(span_end(span))) (len=$(span_len(span)))")
    end
end

"""
    validate_tree(tree::IntervalTree) -> Bool

Validate that the tree maintains its invariants (for debugging).
Returns true if the tree is valid, false otherwise.
"""
function validate_tree(tree::IntervalTree)
    return validate_node(tree.root)
end

validate_node(::Nothing) = true
function validate_node(node::IntervalNode)
    # Check max_end invariant
    expected_max = span_end(node.span)
    if node.left !== nothing
        expected_max = max(expected_max, node.left.max_end)
    end
    if node.right !== nothing
        expected_max = max(expected_max, node.right.max_end)
    end
    
    if node.max_end != expected_max
        println("Invalid max_end at node $(node.span): expected $expected_max, got $(node.max_end)")
        return false
    end
    
    # Check BST property (based on start position)
    if node.left !== nothing && span_start(node.left.span) > span_start(node.span)
        println("BST violation: left child start $(span_start(node.left.span)) > parent start $(span_start(node.span))")
        return false
    end
    
    if node.right !== nothing && span_start(node.right.span) < span_start(node.span)
        println("BST violation: right child start $(span_start(node.right.span)) < parent start $(span_start(node.span))")
        return false
    end
    
    return validate_node(node.left) && validate_node(node.right)
end

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

# Example demonstrating the subtraction algorithm
function example_usage(M)
    println("=== Interval Tree Subtraction Example ===")
    
    # Create initial spans for the minuend
    minuend_spans = [
        M(UInt(100), 50),   # [100, 150)
        M(UInt(200), 40),   # [200, 240)
        M(UInt(300), 60),   # [300, 360)
        M(UInt(10), 30)     # [10, 40)
    ]
    
    # Build the interval tree
    tree = IntervalTree(minuend_spans)
    println("Initial tree:")
    print_tree_spans(tree)
    
    # Define subtrahend spans
    subtrahend_spans = [
        M(UInt(120), 20),   # [120, 140) - overlaps with [100, 150)
        M(UInt(190), 30),   # [190, 220) - overlaps with [200, 240)
        M(UInt(350), 20),   # [350, 370) - overlaps with [300, 360)
        M(UInt(25), 10)     # [25, 35) - overlaps with [10, 40)
    ]
    
    println("\nSubtracting spans:")
    for span in subtrahend_spans
        println("  [$(span_start(span)), $(span_end(span))) (len=$(span_len(span)))")
    end
    
    # Perform the subtraction
    subtract_spans!(tree, subtrahend_spans)
    
    println("\nFinal tree after subtraction:")
    print_tree_spans(tree)
    
    # Validate the tree structure
    if validate_tree(tree)
        println("\n✓ Tree structure is valid")
    else
        println("\n✗ Tree structure is invalid!")
    end
end