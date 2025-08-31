#! /bin/sh

# Linux data-gathering commands; adjust as necessary for your platform.
#
# Be sure to remove any information from the output that would violate
# SC's double-blind review policies.

env | sed "s/$USER/USER/g"
cat /etc/os-release
uname -a
lscpu || cat /proc/cpuinfo
free -h
cat /proc/meminfo
lsblk -a
lspci
lsmod | head -20
julia --project -e 'using InteractiveUtils; versioninfo()'
julia --project -e 'using Pkg; Pkg.status()'
julia --project -e 'using MPI; MPI.versioninfo()'
