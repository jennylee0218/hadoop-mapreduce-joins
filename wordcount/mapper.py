#!/usr/bin/env python3
"""Hadoop Streaming mapper: one line of text in, one 'word<TAB>1' per word out."""
import sys, re

for line in sys.stdin:
    for word in re.findall(r"[a-z']+", line.lower()):
        # One pair per occurrence, on purpose: aggregating here would hide the
        # difference the combiner makes in Part 1c.
        print(f"{word}\t1")
