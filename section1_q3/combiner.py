#!/usr/bin/env python3
"""
Section 1, Q3 - Triangle Counting MapReduce: Combiner

Reads sorted (key, value) pairs from stdin.
Groups by key and counts:
  - "edge" markers: if present, marks the pair as a confirmed edge
  - "wedge" markers: count the number of distinct path-midpoints

Emits: key => "edge:<count_of_edge>" or "wedge:<count_of_wedges>"
This is a local combiner to reduce data before the global shuffle.

Input: sorted lines of "<v,w>\t<tag>"
Output: compressed lines of "<v,w>\tedge" or "<v,w>\twedge:<count>"
"""

import sys


def main():
    current_key = None
    edge_count = 0
    wedge_count = 0

    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 2:
            continue

        key, value = parts[0], parts[1]

        if key != current_key:
            if current_key is not None:
                # Emit combined output for previous key
                if edge_count > 0:
                    print(f"{current_key}\tedge")
                if wedge_count > 0:
                    print(f"{current_key}\twedge:{wedge_count}")
            current_key = key
            edge_count = 0
            wedge_count = 0

        if value == "edge":
            edge_count += 1
        elif value.startswith("wedge"):
            # Handle already-combined wedge:<count>
            if ':' in value:
                wedge_count += int(value.split(':')[1])
            else:
                wedge_count += 1

    # Emit last key
    if current_key is not None:
        if edge_count > 0:
            print(f"{current_key}\tedge")
        if wedge_count > 0:
            print(f"{current_key}\twedge:{wedge_count}")


if __name__ == "__main__":
    main()
