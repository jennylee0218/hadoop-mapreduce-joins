#!/usr/bin/env python3
"""Map-side (broadcast) join, mapper only — no reducer. customers.csv is shipped to every mapper with -files
and loaded into a dict at start-up; each order is joined in memory and emitted immediately.
Emits: customer_id|name<TAB>amount   (one line per order; aggregate afterwards — e.g. with a reducer like wordcount's — if you want totals)"""
import sys, csv

customers = {}
with open("customers.csv", newline="") as f:                 # -files puts it in the task's working directory
    for row in csv.DictReader(f):
        customers[row["customer_id"]] = row["name"]

for line in sys.stdin:
    parts = line.rstrip("\r\n").split(",")
    if parts[0] == "order_id":
        continue
    order_id, customer_id, order_date, amount = parts
    name = customers.get(customer_id)
    if name is None:
        continue                                             # order for a customer we were not given
    print(f"{customer_id}|{name}\t{amount}")
