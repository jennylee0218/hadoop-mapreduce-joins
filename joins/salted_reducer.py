#!/usr/bin/env python3
"""Salted join, step 1 reducer. Input is sorted 'customer_id#salt<TAB>TAG,payload'.

Emits one PARTIAL per (customer, salt):  customer_id<TAB>name<TAB>n<TAB>total
The salt is stripped on output so step 2 can group on customer_id alone.

Totals carry six decimals here, not two. Rounding each of the ten partials to cents and
then summing them would drift by up to half a cent per partial, and Part 2's total has to
reconcile exactly.
"""
import sys

def flush(key, name, n, total):
    if key is not None and name is not None:
        print(f"{key.split('#', 1)[0]}\t{name}\t{n}\t{total:.6f}")

current, name, n, total = None, None, 0, 0.0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    tag, payload = value.split(",", 1)
    if key != current:
        flush(current, name, n, total)
        current, name, n, total = key, None, 0, 0.0
    if tag == "C":
        name = payload
    else:
        n += 1
        total += float(payload)
flush(current, name, n, total)
