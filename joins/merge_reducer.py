#!/usr/bin/env python3
"""Salted join, step 2 reducer. Sums the per-salt partials back to one row per customer.

Input (sorted by customer_id, fed straight through `-mapper cat`):
    customer_id<TAB>name<TAB>n<TAB>total        up to SALTS rows per customer
Output, identical in shape to the unsalted join:
    customer_id<TAB>name<TAB>order_count<TAB>total_amount
"""
import sys

def flush(cid, name, n, total):
    if cid is not None:
        print(f"{cid}\t{name}\t{n}\t{total:.2f}")

current, name, n, total = None, None, 0, 0.0
for line in sys.stdin:
    cid, rest = line.rstrip("\n").split("\t", 1)
    pname, pn, ptotal = rest.split("\t")
    if cid != current:
        flush(current, name, n, total)
        current, name, n, total = cid, None, 0, 0.0
    name = pname
    n += int(pn)
    total += float(ptotal)
flush(current, name, n, total)
