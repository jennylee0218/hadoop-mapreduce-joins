#!/usr/bin/env python3
"""Independent cross-check for the Part 2 joins.

Recomputes customer_id / name / order_count / total_amount straight from the CSVs and
compares it against the TSV a Hadoop job produced. Uses pandas when it is importable
(what the brief asks for) and falls back to the standard library otherwise.

    python3 joins/crosscheck.py --data data --actual out_join_reduce.tsv
    python3 joins/crosscheck.py --data data --actual out_join_map_totals.tsv

The brief asks specifically for a pandas merge, so use --engine pandas when you are
producing that evidence: it fails loudly instead of quietly falling back to the
standard library. --engine auto (the default) prefers pandas and falls back.

Exit status 0 means the two agree. Customers with zero orders are allowed to be absent
from the actual file (the map-side output only has rows where an order existed).
"""
import argparse, csv, sys

TOL = 0.01          # one cent: Hadoop and pandas add the same floats in a different order


def expected_pandas(data):
    import pandas as pd
    cust = pd.read_csv(f"{data}/customers.csv", dtype={"customer_id": str})
    orders = pd.read_csv(f"{data}/orders.csv", dtype={"customer_id": str})
    agg = orders.groupby("customer_id")["amount"].agg(n="count", total="sum")
    m = cust.merge(agg, on="customer_id", how="left").fillna({"n": 0, "total": 0.0})
    return (
        {cid: (name, int(n), float(t))
         for cid, name, n, t in zip(m["customer_id"], m["name"], m["n"], m["total"])},
        "pandas",
    )


def expected_stdlib(data):
    exp = {}
    with open(f"{data}/customers.csv", newline="") as f:
        for row in csv.DictReader(f):
            exp[row["customer_id"]] = [row["name"], 0, 0.0]
    with open(f"{data}/orders.csv", newline="") as f:
        for row in csv.DictReader(f):
            e = exp.get(row["customer_id"])
            if e is not None:
                e[1] += 1
                e[2] += float(row["amount"])
    return {k: tuple(v) for k, v in exp.items()}, "stdlib csv"


def read_actual(path):
    out = {}
    with open(path) as f:
        for ln, line in enumerate(f, 1):
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 4:
                sys.exit(f"{path}:{ln}: expected 4 tab-separated fields, got {len(fields)}")
            cid, name, n, total = fields
            out[cid] = (name, int(n), float(total))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data", help="directory holding customers.csv and orders.csv")
    ap.add_argument("--actual", required=True, help="TSV from a Hadoop job: cid name n total")
    ap.add_argument("--engine", choices=("auto", "pandas", "stdlib"), default="auto",
                    help="auto prefers pandas and falls back; pandas fails if it is not installed")
    a = ap.parse_args()

    if a.engine == "stdlib":
        exp, engine = expected_stdlib(a.data)
    elif a.engine == "pandas":
        try:
            exp, engine = expected_pandas(a.data)
        except ImportError:
            sys.exit("--engine pandas was requested but pandas is not installed here.\n"
                     "Install it (pip install pandas), or run this on the Dataproc master,\n"
                     "or use --engine stdlib and say so in your writeup.")
    else:
        try:
            exp, engine = expected_pandas(a.data)
        except ImportError:
            exp, engine = expected_stdlib(a.data)
    act = read_actual(a.actual)
    print(f"expected rows: {len(exp)} (via {engine})      actual rows: {len(act)}  [{a.actual}]")

    problems, skipped_zero = [], 0
    for cid, (name, n, total) in sorted(exp.items(), key=lambda kv: int(kv[0])):
        if cid not in act:
            if n == 0:
                skipped_zero += 1
            else:
                problems.append(f"  missing customer {cid} ({name}): expected n={n} total={total:.2f}")
            continue
        aname, an, atotal = act[cid]
        if aname != name:
            problems.append(f"  customer {cid}: name '{aname}' != '{name}'")
        if an != n:
            problems.append(f"  customer {cid}: order_count {an} != {n}")
        if abs(atotal - total) > TOL:
            problems.append(f"  customer {cid}: total {atotal:.2f} != {total:.2f}")
    for cid in act:
        if cid not in exp:
            problems.append(f"  unexpected customer {cid} in {a.actual}")

    tot_exp = sum(v[2] for v in exp.values())
    tot_act = sum(v[2] for v in act.values())
    n_exp = sum(v[1] for v in exp.values())
    n_act = sum(v[1] for v in act.values())
    print(f"grand total amount  expected {tot_exp:,.2f}   actual {tot_act:,.2f}")
    print(f"grand total orders  expected {n_exp:,}   actual {n_act:,}")
    if skipped_zero:
        print(f"({skipped_zero} customer(s) with zero orders absent from the actual file — allowed)")

    if problems:
        print(f"\nMISMATCH — {len(problems)} problem(s), first 20:")
        print("\n".join(problems[:20]))
        return 1
    print("\nOK — every row reconciles.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
