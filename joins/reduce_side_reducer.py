#!/usr/bin/env python3
"""Reduce-side join, reducer. All records for one customer_id arrive together (sorted by key).
Hold the customer's name, count the orders, sum the amounts, and emit: customer_id<TAB>name<TAB>order_count<TAB>total_amount
(Note the tag trick: because 'C' < 'O', sort the values or buffer them — the customer record is not guaranteed to come first.)"""
import sys

def flush(cid, name, n, total):
    if cid is not None and name is not None:
        print(f"{cid}\t{name}\t{n}\t{total:.2f}")

current, name, n, total = None, None, 0, 0.0
for line in sys.stdin:
    cid, value = line.rstrip("\n").split("\t", 1)
    tag, payload = value.split(",", 1)
    if cid != current:
        flush(current, name, n, total)
        current, name, n, total = cid, None, 0, 0.0
    # Value order inside a group is not guaranteed, but we only print at the group
    # boundary above, so the name is in hand by then wherever the C record landed.
    if tag == "C":
        name = payload
    else:
        n += 1
        total += float(payload)
flush(current, name, n, total)
