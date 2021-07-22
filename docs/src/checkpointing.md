# Checkpointing

If at some point during a Dagger computation a thunk throws an error, or if the
entire computation dies because the head node hit an OOM or other unexpected
error, the entire computation is lost and needs to be started from scratch.
This can be unacceptable for scheduling very large/expensive/mission-critical
graphs, and for interactive development where errors are common and easily
fixable.

Robust applications often support "checkpointing", where intermediate results
are periodically written out to persistent media, or sharded to the rest of the
cluster, to allow resuming an interrupted computation from a point later than
the original start. Dagger provides infrastructure to perform user-driven
checkpointing of intermediate results once they're generated.

As a concrete example, imagine that you're developing a numerical algorithm,
and distributing it with Dagger. The idea is to sum all the values in a very
big matrix, and then get the square root of the absolute value of the sum of
sums. Here is what that might look like:

```julia
X = compute(randn(Blocks(128,128), 1024, 1024))
Y = [delayed(sum)(chunk) for chunk in X.chunks]
inner(x...) = sqrt(sum(x))
Z = delayed(inner)(Y...)
z = collect(Z)
```

Let's pretend that the above calculation of each element in `Y` takes a full
day to run. If you run this, you might realize that if the final `sum` call
returns a negative number, `sqrt` will throw a `DomainError` (because `sqrt`
can't accept negative `Real` inputs). Of course, you forgot to add a call to
`abs` before the call to `sqrt`! Now, you know how to fix this, but once you
do, you'll have to spend another entire day waiting for it to finish! And maybe
you fix this one bug and wait a full day for it to finish, and begin adding
more very computationally-heavy code (which inevitably has bugs). Those later
computations might fail, and if you're running this as a script (maybe under a
cluster scheduler like Slurm), you have to restart everything from the very
beginning. This is starting to sound pretty wasteful...

Thankfully, Dagger has a simple solution to this: checkpointing. With
checkpointing, Dagger can be instructed to save intermediate results (maybe the
results of computing `Y`) to a persistent storage medium of your choice.
Probably a file on disk, but maybe a database, or even just stored in RAM in a
space-efficient form. You also tell Dagger how to restore this data: how to
take the result stored in its persistent form, and turn it back into something
identical to the original intermediate data that Dagger computed. Then, when
the worst happens and a piece of your algorithm throws an error (as above),
Dagger will call the restore function and try to materialize those intermediate
results that you painstakingly computed, so that you don't need to re-compute
them.

Let's see how we'd modify the above example to use checkpointing:

```julia
using Serialization
X = compute(randn(Blocks(128,128), 1024, 1024))
Y = [delayed(sum; options=Dagger.Sch.ThunkOptions(;
checkpoint=(thunk,result)->begin
    open("checkpoint-$idx.bin", "w") do io
        serialize(io, collect(result))
    end
end, restore=(thunk)->begin
    open("checkpoint-$idx.bin", "r") do io
        Dagger.tochunk(deserialize(io))
    end
end))(chunk) for (idx,chunk) in enumerate(X.chunks)]
inner(x...) = sqrt(sum(x))
Z = delayed(inner)(Y...)
z = collect(Z)
```

Two changes were made: first, we `enumerate(X.chunks)` so that we can get a
unique index to identify each `chunk`; second, we specify a `ThunkOptions` to
`delayed` with a `checkpoint` and `restore` function that is specialized to
write or read the given chunk to or from a file on disk, respectively. Notice
the usage of `collect` in the `checkpoint` function, and the use of
`Dagger.tochunk` in the restore function; Dagger represents intermediate
results as `Dagger.Chunk` objects, so we need to convert between `Chunk`s
and the actual data to keep Dagger happy. Performance-sensitive users might
consider modifying these methods to store the checkpoint files on the
filesystem of the server that currently owns the `Chunk`, to minimize data
transfer times during checkpoint and restore operations.

If we run the above code once, we'll still end up waiting a day for `Y` to be
computed, and we'll still get the `DomainError` from `sqrt`. However, when we
fix the `inner` function to include that call to `abs` that was missing, and we
re-run this code starting from the creation of `Y`, we'll find that we don't
actually spend a day waiting; we probably spend a few seconds waiting, and end
up with our final result! This is because Dagger called the `restore` function
for each element of `Y`, and was provided a result by the user-specified
function, so it skipped re-computing those sums entirely.

You might also notice that when you ran this code the first time, you received
errors about "No such file or directory", or some similar error; this occurs
because Dagger *always* calls the restore function when it exists. In the first
run, the checkpoint files don't yet exist, so there's nothing to restore;
Dagger reports the thrown error, but keeps moving along, merrily computing the
sums of `Y`. You're welcome to explicitly check if the file exists, and if not,
return `nothing`; then Dagger won't report an annoying error, and will skip the
restoration quietly.

Of course, you might have a lot of code that looks like this, and may want to
also checkpoint the final result of the `z = collect(...)` call as well. This
is just as easy to do:

```julia
# compute X, Y, Z above ...
z = collect(Z; options=Dagger.Sch.SchedulerOptions(;
checkpoint=(result)->begin
    open("checkpoint-final.bin", "w") do io
        serialize(io, collect(result))
    end
end, restore=()->begin
    open("checkpoint-final.bin", "r") do io
        Dagger.tochunk(deserialize(io))
    end
end))
```

In this case, the entire computation will be skipped if `checkpoint-final.bin`
exists!
