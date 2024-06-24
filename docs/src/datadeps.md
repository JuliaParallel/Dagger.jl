# Datadeps (Data Dependencies)

For many programs, the restriction that tasks cannot write to their arguments
feels overly restrictive and makes certain kinds of programs (such as in-place
linear algebra) hard to express efficiently in Dagger. Thankfully, there is a
solution called "Datadeps" (short for "data dependencies"), accessible through
the `spawn_datadeps` function. This function constructs a "datadeps region",
within which tasks are allowed to write to their arguments, with parallelism
controlled via dependencies specified via argument annotations. Let's look at a
simple example to make things concrete:

```julia
A = rand(1000)
B = rand(1000)
C = zeros(1000)
add!(X, Y) = X .+= Y
Dagger.spawn_datadeps() do
    Dagger.@spawn add!(InOut(B), In(A))
    Dagger.@spawn copyto!(Out(C), In(B))
end
```

In this example, we have two Dagger tasks being launched, one adding `A` into
`B`, and the other copying `B` into `C`. The `add!` task is specifying that
`A` is being only read from (`In` for "input"), and that `B` is being read
from and written to (`Out` for "output", `InOut` for "input and output"). The
`copyto` task, similarly, is specifying that `B` is being read from, and `C`
is only being written to.

Without `spawn_datadeps` and `In`, `Out`, and `InOut`, the result of these
tasks would be undefined; the two tasks could execute in parallel, or the
`copyto!` could occur before the `add!`, resulting in all kinds of mayhem.
However, `spawn_datadeps` changes things: because we have told Dagger how our
tasks access their arguments, Dagger knows to control the parallelism and
ordering, and ensure that `add!` executes and finishes before `copyto!`
begins, ensuring that `copyto!` "sees" the changes to `B` before executing.

There is another important aspect of `spawn_datadeps` that makes the above
code work: if all of the `Dagger.@spawn` macros are removed, along with the
dependency specifiers, the program would still produce the same results,
without using Dagger. In other words, the parallel (Dagger) version of the
program produces identical results to the serial (non-Dagger) version of the
program. This is similar to using Dagger with purely functional tasks and
without `spawn_datadeps` - removing `Dagger.@spawn` will still result in a
correct (sequential and possibly slower) version of the program. Basically,
`spawn_datadeps` will ensure that Dagger respects the ordering and
dependencies of a program, while still providing parallelism, where possible.

But where is the parallelism? The above example doesn't actually have any
parallelism to exploit! Let's take a look at another example to see the
datadeps model truly shine:

```julia
# Tree reduction of multiple arrays into the first array
function tree_reduce!(op::Base.Callable, As::Vector{<:Array})
    Dagger.spawn_datadeps() do
        to_reduce = Vector[]
        push!(to_reduce, As)
        while !isempty(to_reduce)
            As = pop!(to_reduce)
            n = length(As)
            if n == 2
                Dagger.@spawn Base.mapreducedim!(identity, op, InOut(As[1]), In(As[2]))
            elseif n > 2
                push!(to_reduce, [As[1], As[div(n,2)+1]])
                push!(to_reduce, As[1:div(n,2)])
                push!(to_reduce, As[div(n,2)+1:end])
            end
        end
    end
    return As[1]
end

As = [rand(1000) for _ in 1:1000]
Bs = copy.(As)
tree_reduce!(+, As)
@assert isapprox(As[1], reduce((x,y)->x .+ y, Bs))
```

In the above implementation of `tree_reduce!` (which is designed to perform an
elementwise reduction across a vector of arrays), we have a tree reduction
operation where pairs of arrays are reduced, starting with neighboring pairs,
and then reducing pairs of reduction results, etc. until the final result is in
`As[1]`. We can see that the application of Dagger to this algorithm is simple -
only the single `Base.mapreducedim!` call is passed to Dagger - yet due to the
data dependencies and the algorithm's structure, there should be plenty of
parallelism to be exploited across each of the parallel reductions at each
"level" of the reduction tree. Specifically, any two `Dagger.@spawn` calls
which access completely different pairs of arrays can execute in parallel,
while any call which has an `In` on an array will wait for any previous call
which has an `InOut` on that same array.

Additionally, we can notice a powerful feature of this model - if the
`Dagger.@spawn` macro is removed, the code still remains correct, but simply
runs sequentially. This means that the structure of the program doesn't have to
change in order to use Dagger for parallelization, which can make applying
Dagger to existing algorithms quite effortless.

## Aliasing Support

Datadeps is smart enough to detect when two arguments from different tasks
actually access the same memory (we say that these arguments "alias"). There's
the obvious case where the two arguments are exactly the same object, but
Datadeps is also aware of more subtle cases, such as when two arguments are
different views into the same array, or where two arrays point to the same
underlying memory. In these cases, Datadeps will ensure that the tasks are
executed in the correct order - if one task writes to an argument which aliases
with an argument read by another task, those two tasks will be executed in
sequence, rather than in parallel.

There are two ways to specify aliasing to Datadeps. The simplest way is the most straightforward: if the argument passed to a task is a view or another supported object (such as an `UpperTriangular`-wrapped array), Datadeps will compare it with all other task's arguments to determine if they alias. This works great when you want to pass that view or `UpperTriangular` object directly to the called function. For example:

```julia
A = rand(1000)
A_l = view(A, 1:500)
A_r = view(A, 501:1000)

# inc! supports views, so we can pass A_l and A_r directly
inc!(X) = X .+= 1

Dagger.spawn_datadeps() do
    # These two tasks don't alias, so they can run in parallel
    Dagger.@spawn inc!(InOut(A_l))
    Dagger.@spawn inc!(InOut(A_r))

    # This task aliases with the previous two, so it will run after them
    Dagger.@spawn inc!(InOut(A))
end
```

The other way allows you to separate what argument is passed to the function,
from how that argument is accessed within the function. This is done with the
`Deps` wrapper, which is used like so:

```julia
A = rand(1000, 1000)

inc_upper!(X) = UpperTriangular(X) .+= 1
inc_ulower!(X) = UnitLowerTriangular(X) .+= 1
inc_diag!(X) = X[diagind(X)] .+= 1

Dagger.spawn_datadeps() do
    # These two tasks don't alias, so they can run in parallel
    Dagger.@spawn inc_upper!(Deps(A, InOut(UpperTriangular)))
    Dagger.@spawn inc_ulower!(Deps(A, InOut(UnitLowerTriangular)))

    # This task aliases with the `inc_upper!` task (`UpperTriangular` accesses the diagonal of the array)
    Dagger.@spawn inc_diag!(Deps(A, InOut(Diagonal)))
end
```

You can pass any number of aliasing modifiers to `Deps`. This is particularly
useful for declaring aliasing with `Diagonal`, `Bidiagonal`, `Tridiagonal`, and
`SymTridiagonal` access, as these "wrappers" make a copy of their parent array
and thus can't be used to "mask" access to the parent like `UpperTriangular`
and `UnitLowerTriangular` can (which is valuable for writing memory-efficient,
generic algorithms in Julia).
