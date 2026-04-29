#!/usr/bin/env python3
"""
Parse MPI+Dagger logs and report communication decision asymmetry per tag.
Asymmetry: for the same tag, one rank decides to send (local+bcast, etc.)
and another decides to infer (inferred) and never recv → deadlock.

Usage:
  # Capture full log (all ranks' Core.println from mpi.jl go to stdout):
  mpiexec -n 10 julia --project=/path/to/Dagger.jl benchmarks/run_matmul.jl 2>&1 | tee matmul.log
  # Then look for asymmetry (same tag: one rank sends, another infers → deadlock):
  python3 check_comm_asymmetry.py < matmul.log
"""

import re
import sys
from collections import defaultdict

SEND_DECISIONS = {"local+bcast", "sender+communicated", "sender+inferred", "receiver+bcast"}
RECV_DECISIONS = {"communicated", "receiver", "sender+communicated"}
INFER_DECISIONS = {"inferred", "uninvolved", "sender+inferred"}


def parse_line(line: str):
    rank = tag = category = decision = None
    m = re.search(r"\[rank\s+(\d+)\]", line)
    if m:
        rank = int(m.group(1))
    m = re.search(r"\[tag\s+(\d+)\]", line)
    if m:
        tag = int(m.group(1))
    m = re.search(r"\[(execute!|aliasing|remotecall_endpoint)\]", line)
    if m:
        category = m.group(1)
    # Capture decision from [...] blocks
    for m in re.finditer(r"\]\[([^\]]+)\]", line):
        candidate = m.group(1)
        if "inferred" in candidate and "communicated" not in candidate:
            decision = "inferred"
            break
        if "communicated" in candidate:
            decision = "communicated"
            break
        if "local+bcast" in candidate:
            decision = "local+bcast"
            break
        if candidate.startswith("sender+"):
            decision = "sender+inferred" if "inferred" in candidate else "sender+communicated"
            break
        if candidate == "receiver":
            decision = "receiver"
            break
        if candidate == "receiver+bcast":
            decision = "receiver+bcast"
            break
        if candidate == "inplace_move":
            decision = "inplace_move"
            break
    return rank, tag, category, decision


def main():
    by_tag = defaultdict(dict)  # tag -> {rank: decision}
    for line in sys.stdin:
        rank, tag, category, decision = parse_line(line)
        if rank is None or tag is None or decision is None:
            continue
        by_tag[tag][rank] = decision

    send_keys = {"local+bcast", "sender+communicated", "sender+inferred", "receiver+bcast"}
    infer_keys = {"inferred", "sender+inferred", "uninvolved"}
    recv_keys = {"communicated", "receiver", "sender+communicated"}

    asymmetries = []
    for tag in sorted(by_tag.keys()):
        ranks = by_tag[tag]
        senders = [r for r, d in ranks.items() if d in send_keys]
        inferrers = [r for r, d in ranks.items() if d in infer_keys]
        receivers = [r for r, d in ranks.items() if d in recv_keys]
        if senders and inferrers:
            asymmetries.append((tag, senders, inferrers, receivers, ranks))

    if not asymmetries:
        print("No communication decision asymmetry found (no tag has both sender and inferrer).")
        return

    print("=== Communication decision asymmetry (can cause deadlock) ===\n")
    for tag, senders, inferrers, receivers, ranks in asymmetries:
        print(f"Tag {tag}:")
        print(f"  Senders (will bcast to all others): {senders}")
        print(f"  Inferrers (did not recv): {inferrers}")
        print(f"  Receivers: {receivers}")
        print(f"  All decisions: {dict(ranks)}")
        print()


if __name__ == "__main__":
    main()
