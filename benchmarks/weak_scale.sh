#!/bin/bash

# Define the output CSV file
OUTPUT_FILE="weak_scale_results.csv"

# Check if the CSV file exists. If not, create it with a header.
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "benchmark,procs,dtype,size,blocksize,time,gflops" > "$OUTPUT_FILE"
fi

# --- Run the DaggerTCP benchmark ---
echo "Running DaggerTCP_Weak_scale.jl..."

# Run the Julia script and use 'sed' to prepend the benchmark name to each line of output.
julia --project=. benchmarks/DaggerTCP_Weak_scale.jl | sed 's/^/DaggerTCP_Weak_scale,/' >> "$OUTPUT_FILE"

# --- Run the DaggerMPI benchmark ---
echo "Running DaggerMPI benchmark..."

# Run the MPI command and use 'sed' to prepend the benchmark name to each line of output.
mpiexec -n 4 julia --project=. -e 'include("benchmarks/mpi_dagger_bench.jl")' | sed 's/^/DaggerMPI_Weak_scale,/' >> "$OUTPUT_FILE"

echo "Weak scale benchmarks complete. Results are in $OUTPUT_FILE"