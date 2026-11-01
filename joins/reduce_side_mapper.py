#!/usr/bin/env python3
"""Reduce-side join, mapper. Reads BOTH files (Hadoop feeds them to the same mapper); tags each record so the
reducer can tell customers from orders. Emits: customer_id<TAB>TAG,payload

  customers.csv: customer_id,name,state,segment   -> key=customer_id  value=C,name
  orders.csv:    order_id,customer_id,order_date,amount -> key=customer_id value=O,amount
"""
import sys, os

fname = os.environ.get("mapreduce_map_input_file", "")     # Hadoop tells you which file this split came from
is_customers = "customers" in fname
for line in sys.stdin:
    parts = line.rstrip("\r\n").split(",")
    if parts[0] in ("customer_id", "order_id"):
        continue                                            # header rows
    if is_customers:
        print(f"{parts[0]}\tC,{parts[1]}")                  # customer_id, name
    else:
        print(f"{parts[1]}\tO,{parts[3]}")                  # customer_id, amount
