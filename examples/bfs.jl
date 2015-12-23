
addprocs(2)

# Map function for BFS. Returns all vertices reachable from the input vertex
@everywhere function mapBFS(t)
  u = t[1]
  d = t[2]
  [(v,d+1) for v::Int in find(sparseMatrix[u,:])]
end

# Filter function for BFS. Discard already-explored vertices
@everywhere function filterBFS(tuple)
  u,d = tuple
  if dist[u] < 0
    dist[u] = d
    return true
  else
    return false
  end
end


using ComputeFramework

@everywhere numVertices = 8
@everywhere adjacencyMatrix =  [ 0  1  1  1  0  0  0  0;
                                 1  0  1  0  1  1  0  0;
                                 1  1  0  1  0  1  1  0;
                                 1  0  1  0  0  0  1  0;
                                 0  1  0  0  0  1  0  1;
                                 0  1  1  0  1  0  1  1;
                                 0  0  1  1  0  1  0  1;
                                 0  0  0  0  1  1  1  0;
                                ]

@everywhere sparseMatrix = sparse(adjacencyMatrix)

#place distance vector in shared memory and initialize it in parallel.
ldist = SharedArray(Int, numVertices, init = dist -> dist[Base.localindexes(dist)] = -1)
@everywhere dist = nothing
for p in procs()
  @spawnat p (global dist;dist = ldist)
end
yield()

# We could also use multiple seeds
dist[1] = 0
seedVertex = 1
Q = [(seedVertex,0)]

# Main BFS loop that implements iterative map-reduce-filter
while length(Q) > 0
  s1 = map(mapBFS, distribute(Q))    # explore new vertices
  s2 = reduce(vcat, [], s1)          # concatenate the lists of tuples produced in map stage
  s3 = compute(Context(), s2)
  Q = gather(Context(), filter(filterBFS, distribute(s3)))
  println(Q)
end

println(dist)
