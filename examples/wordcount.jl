addprocs(3)

using ComputeFramework

words = ["one", "two", "two", "three", "three", "three", "four", "four", "four", "four"]

parallel_words = distribute(words)
count_one = map(x -> x => 1, parallel_words)
wcount = reducebykey(+, 0, count_one)

@show compute(Context(), wcount)
