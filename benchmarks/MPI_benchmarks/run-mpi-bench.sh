#!/bin/sh

set -eux

CMD=$1
NP=$2

julia --project -e "using MPI; run(\`\$(mpiexec()) -np $NP julia --project $CMD\`)"
