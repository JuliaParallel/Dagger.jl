#!/bin/bash

set -eux

CMD="benchmarks/MPI_benchmarks/weak_scaling/DaggerMPI_Weak_scale.jl"
BENCHMARK_NAME="DaggerMPI_Weak_scale"
OUTPUT_FILE="benchmarks/MPI_benchmarks/scaling_results/weak_scale_results.csv"


# Create the CSV header if the file doesn't exist.
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "benchmark,procs,dtype,size,time,gflops" > "$OUTPUT_FILE"
fi

for procs in 1 4 9 16 25 36 49 64 81; do
    echo "Running $BENCHMARK_NAME with $procs processes..."

    julia --project -e "using MPI; run(\`\$(mpiexec()) -np $procs julia --project $CMD\`)" | sed "s/^/$BENCHMARK_NAME,/" >> "$OUTPUT_FILE"
done

# Run the TCP benchmark
DAGGERTCP_NAME="DaggerTCP_Weak_scale"
julia --project benchmarks/MPI_benchmarks/weak_scaling/DaggerTCP_Weak_scale.jl | sed "s/^/$DAGGERTCP_NAME,/" >> "$OUTPUT_FILE"

echo "All benchmarks are complete. Results are in $OUTPUT_FILE"