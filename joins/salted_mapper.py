#!/usr/bin/env python3
"""Salted reduce-side join, step 1 mapper (Part 4.2).

One hot customer_id otherwise sends all its orders to a single reducer. Salting spreads
them over SALTS reducers instead:

  orders.csv    -> key = customer_id#<one random salt>   value = O,amount
  customers.csv -> key = customer_id#<EVERY salt>        value = C,name   (SALTS copies)

Replicating the small side is the step that is easy to get wrong. If a customers row went
to only one salt, the other nine groups for that customer would arrive with no name and
the reducer would drop every order in them. tests/test_salted_join.py guards exactly that.
"""
import sys, os, random

SALTS = 10

fname = os.environ.get("mapreduce_map_input_file", "")
is_customers = "customers" in fname
rng = random.Random()                       # the brief asks for a random salt per order

for line in sys.stdin:
    parts = line.rstrip("\r\n").split(",")
    if parts[0] in ("customer_id", "order_id"):
        continue                            # header rows
    if is_customers:
        for salt in range(SALTS):           # the small side is replicated, not salted
            print(f"{parts[0]}#{salt}\tC,{parts[1]}")
    else:
        print(f"{parts[1]}#{rng.randrange(SALTS)}\tO,{parts[3]}")
