#!/usr/bin/env python3
"""Hadoop Streaming reducer (also used as the combiner): input is SORTED 'word<TAB>count' lines;
sum the counts for each run of identical words and emit 'word<TAB>total'."""
import sys

current, total = None, 0
for line in sys.stdin:
    word, count = line.rstrip("\n").split("\t", 1)
    if word == current:
        # Read the incoming count instead of adding 1: as a combiner this script
        # eats its own output, so counts arriving here are not always 1.
        total += int(count)
    else:
        if current is not None:
            print(f"{current}\t{total}")
        current, total = word, int(count)
if current is not None:
    print(f"{current}\t{total}")
