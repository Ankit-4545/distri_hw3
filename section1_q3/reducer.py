#!/usr/bin/env python3
"""
Section 1, Q3 - Triangle Counting MapReduce: Reducer

Reads sorted (key, value) pairs from stdin.
For each edge pair (v, w):
  - If we see an "edge" marker AND "wedge:<count>" markers,
    it means there exist <count> common nodes u such that
    v-u-w-v forms a triangle.
  - The total triangle count is divided by 3 at the end to avoid
    counting each triangle 3 times (once per vertex).

Wait — actually in this approach:
  A triangle {a, b, c} contributes:
    - wedge at key (b,c) from node a
    - wedge at key (a,c) from node b
    - wedge at key (a,b) from node c
  AND edge markers for (a,b), (a,c), (b,c)

  Each triangle is counted once per edge-closing pair.
  So for triangle {a,b,c}:
    - key=(b,c): edge marker + wedge from a => count 1
    - key=(a,c): edge marker + wedge from b => count 1
    - key=(a,b): edge marker + wedge from c => count 1
  => total raw count = 3, divide by 3.

Input: sorted lines of "<v,w>\t<edge|wedge:N>"
Output: a single integer (total triangle count)
"""

import sys


def main():
    current_key = None
    has_edge = False
    total_wedges = 0
    raw_triangle_count = 0

    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 2:
            continue

        key, value = parts[0], parts[1]

        if key != current_key:
            if current_key is not None and has_edge and total_wedges > 0:
                raw_triangle_count += total_wedges
            current_key = key
            has_edge = False
            total_wedges = 0

        if value == "edge":
            has_edge = True
        elif value.startswith("wedge"):
            if ':' in value:
                total_wedges += int(value.split(':')[1])
            else:
                total_wedges += 1

    # Process last key
    if current_key is not None and has_edge and total_wedges > 0:
        raw_triangle_count += total_wedges

    # Each triangle was counted 3 times (once per edge)
    print(raw_triangle_count // 3)


if __name__ == "__main__":
    main()
