addprocs(3)

using Dagger

words = ["one", "two", "two", "three", "three", "three", "four", "four", "four", "four"]

parallel_words = compute(Distribute(BlockPartition(1), words))
count_one = map(x -> x => 1, parallel_words)
wcount = reducebykey(+, count_one)

@show compute(wcount)
