using Random
import Dagger: IntervalTree, LocalMemorySpan, span_start, span_end, span_len, find_overlapping, subtract_spans!

const IT = Dagger
mkspan(s, l) = LocalMemorySpan(UInt(s), UInt(l))
spans_of(tree) = sort!([(Int(span_start(x)), Int(span_len(x))) for x in tree])

# search order (ties may sit on either side), heap order of the priorities and `max_end` of every node
function check_tree(tree)
    function walk(node, lo, hi)
        node === nothing && return (true, UInt64(0))
        s = span_start(node.span)
        ok = s >= lo && s <= hi
        for child in (node.left, node.right)
            child === nothing || (ok &= node.prio >= child.prio)
        end
        lok, lmax = walk(node.left, lo, s)
        rok, rmax = walk(node.right, s, hi)
        m = max(span_end(node.span), lmax, rmax)
        return (ok && lok && rok && node.max_end == m, m)
    end
    return walk(tree.root, UInt64(0), typemax(UInt64))[1]
end
depth(node) = node === nothing ? 0 : 1 + max(depth(node.left), depth(node.right))

@testset "Interval tree" begin
    @testset "balanced for spans in address order" begin
        tree = IntervalTree{LocalMemorySpan}()
        for i in 1:20000
            insert!(tree, mkspan(1 + 200 * i, 64))
        end
        @test check_tree(tree)
        @test depth(tree.root) <= 60          # a chain would be 20000 deep
        @test length(tree) == 20000
    end

    @testset "same operations, same shape" begin
        build() = (t = IntervalTree{LocalMemorySpan}(); for i in 1:500; insert!(t, mkspan(1 + 37 * ((i * 7919) % 500), 8)); end; t)
        @test collect(build()) == collect(build())
    end

    @testset "random inserts and exact deletes against a list" begin
        rng = MersenneTwister(7)
        for trial in 1:30
            tree = IntervalTree{LocalMemorySpan}()
            ref = Tuple{Int,Int}[]
            for step in 1:300
                if isempty(ref) || rand(rng) < 0.65
                    s = rand(rng, 1:60) * 4; l = rand(rng, 1:4) * 2      # many equal starts, different lengths
                    insert!(tree, mkspan(s, l)); push!(ref, (s, l))
                else
                    k = rand(rng, eachindex(ref)); s, l = ref[k]
                    deleteat!(ref, k)
                    delete!(tree, mkspan(s, l))
                end
                step % 50 == 0 && @test check_tree(tree)
            end
            @test spans_of(tree) == sort(ref)
        end
    end

    @testset "overlap queries against brute force" begin
        rng = MersenneTwister(11)
        for trial in 1:20
            tree = IntervalTree{LocalMemorySpan}()
            ref = Tuple{Int,Int}[]
            for _ in 1:200
                s = rand(rng, 1:400); l = rand(rng, 1:20)
                insert!(tree, mkspan(s, l)); push!(ref, (s, l))
            end
            for _ in 1:50
                qs = rand(rng, 1:420); ql = rand(rng, 1:30)
                got = sort!([(Int(span_start(x)), Int(span_len(x))) for x in find_overlapping(tree, mkspan(qs, ql); exact=false)])
                want = sort!([(s, l) for (s, l) in ref if s <= qs + ql - 1 && qs <= s + l - 1])
                @test got == want
            end
        end
    end

    @testset "subtraction removes exactly the covered bytes" begin
        rng = MersenneTwister(5)
        for trial in 1:30
            tree = IntervalTree{LocalMemorySpan}()
            covered = falses(600)
            for _ in 1:20          # disjoint minuend, like the planner's remainder
                s = rand(rng, 1:560)
                l = rand(rng, 1:30)
                any(covered[s:min(s + l - 1, 600)]) && continue
                covered[s:min(s + l - 1, 600)] .= true
                insert!(tree, mkspan(s, min(l, 600 - s + 1)))
            end
            subs = [mkspan(rand(rng, 1:560), rand(rng, 1:40)) for _ in 1:8]
            for sub in subs
                a = Int(span_start(sub)); b = Int(span_end(sub))
                covered[a:min(b, 600)] .= false
            end
            subtract_spans!(tree, subs)
            @test check_tree(tree)
            got = falses(600)
            for x in tree
                got[Int(span_start(x)):Int(span_end(x))] .= true
            end
            @test got == covered
        end
    end
end
