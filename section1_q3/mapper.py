#!/usr/bin/env python3
"""
Section 1, Q3 - Triangle Counting MapReduce: Mapper

Strategy: Node-Iterator / Partition-by-Node approach
- Read ALL edges first to build an adjacency list for the chunk.
- For every edge (u, v), we need to check if they share a common neighbor.
- We emit (sorted triple) for each pair of neighbors of a node,
  signaling that this is a "wedge" that could close into a triangle.

MapReduce approach used here:
  1. Mapper reads all edges.
  2. For each node u with neighbors [v1, v2, ...],
     for every pair (vi, vj) emit key=(min(vi,vj), max(vi,vj)) => value=u
     This means: "there is a path vi - u - vj"
  3. Also emit the edge itself: key=(min(u,v), max(u,v)) => value="edge"
  4. Reducer: for each pair (vi, vj), if we see "edge" AND some node u,
     it means vi - u - vj - vi is a triangle. Count distinct u values.

Input (stdin): first line is "V E", then E lines of "u v"
Output (stdout): tab-separated key-value pairs
"""

import sys
from collections import defaultdict


def main():
    lines = sys.stdin.read().split('\n')
    lines = [l.strip() for l in lines if l.strip()]

    if not lines:
        return

    # Parse header
    header = lines[0].split()
    V, E = int(header[0]), int(header[1])

    # Build adjacency list from the chunk
    adj = defaultdict(set)
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 2:
            continue
        u, v = int(parts[0]), int(parts[1])
        if u == v:
            continue  # skip self-loops
        adj[u].add(v)
        adj[v].add(u)

    # For each node u, emit:
    #   1. For each edge (u, v): emit "edge\t(min(u,v),max(u,v))"
    #   2. For each pair of neighbors (v, w) of u: emit "u\t(min(v,w),max(v,w))"
    #      meaning: there is a path v-u-w (a wedge)

    emitted_edges = set()

    for u in adj:
        neighbors = sorted(adj[u])
        # Emit edge markers
        for v in neighbors:
            edge_key = (min(u, v), max(u, v))
            if edge_key not in emitted_edges:
                emitted_edges.add(edge_key)
                print(f"{edge_key[0]},{edge_key[1]}\tedge")

        # Emit wedge markers: for every pair of neighbors
        for i in range(len(neighbors)):
            for j in range(i + 1, len(neighbors)):
                v = neighbors[i]
                w = neighbors[j]
                key = (min(v, w), max(v, w))
                print(f"{key[0]},{key[1]}\twedge")


if __name__ == "__main__":
    main()
