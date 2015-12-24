#addprocs(1)

using ComputeFramework
using Elly

#hfile = HDFSFile(clnt, "/twitter_small.csv")
words = split(HDFSFileNode("localhost", 9000, "/colsuminp.csv"), ',')

count_one = map(x -> x => 1, words)
wcount = reducebykey(+, 0, count_one)

@show compute(Context(), wcount)
