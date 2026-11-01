#!/usr/bin/env python3
"""Part 4.3: the same customer/order join as a single local process, for timing
against the two Hadoop jobs.

    time python3 join_local.py --data ../data --out out_join_local.tsv

Writes the identical four columns the Hadoop jobs write, so crosscheck.py can verify it:
    customer_id <TAB> name <TAB> order_count <TAB> total_amount

Uses pandas when importable (what the brief asks for), standard-library csv otherwise.
Reports peak RSS at the end, because Part 4.3b asks at what memory-to-data ratio the
cluster starts to win -- that answer needs a measured memory number, not a guess.
"""
import argparse, csv, resource, sys, time


def join_pandas(data):
    import pandas as pd
    cust = pd.read_csv(f"{data}/customers.csv", dtype={"customer_id": str})
    orders = pd.read_csv(f"{data}/orders.csv", dtype={"customer_id": str})
    agg = orders.groupby("customer_id")["amount"].agg(n="count", total="sum")
    m = cust.merge(agg, on="customer_id", how="left").fillna({"n": 0, "total": 0.0})
    return [(cid, name, int(n), float(t))
            for cid, name, n, t in zip(m["customer_id"], m["name"], m["n"], m["total"])], "pandas"


def join_stdlib(data):
    out = {}
    with open(f"{data}/customers.csv", newline="") as f:
        for row in csv.DictReader(f):
            out[row["customer_id"]] = [row["name"], 0, 0.0]
    with open(f"{data}/orders.csv", newline="") as f:
        for row in csv.DictReader(f):
            e = out.get(row["customer_id"])
            if e is not None:
                e[1] += 1
                e[2] += float(row["amount"])
    return [(cid, v[0], v[1], v[2]) for cid, v in out.items()], "stdlib csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="../data")
    ap.add_argument("--out", default="out_join_local.tsv")
    ap.add_argument("--engine", choices=("auto", "pandas", "stdlib"), default="auto")
    a = ap.parse_args()

    t0 = time.time()
    if a.engine == "stdlib":
        result, engine = join_stdlib(a.data)
    elif a.engine == "pandas":
        result, engine = join_pandas(a.data)
    else:
        try:
            result, engine = join_pandas(a.data)
        except ImportError:
            result, engine = join_stdlib(a.data)

    result.sort(key=lambda r: r[0])
    with open(a.out, "w") as f:
        for cid, name, n, total in result:
            f.write(f"{cid}\t{name}\t{n}\t{total:.2f}\n")
    elapsed = time.time() - t0

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mb = peak_kb / 1024 if sys.platform.startswith("linux") else peak_kb / 1024 / 1024
    print(f"engine            {engine}")
    print(f"rows              {len(result):,}")
    print(f"total orders      {sum(r[2] for r in result):,}")
    print(f"total amount      {sum(r[3] for r in result):,.2f}")
    print(f"elapsed           {elapsed:.2f} s")
    print(f"peak memory       {peak_mb:,.0f} MB")
    print(f"wrote             {a.out}")


if __name__ == "__main__":
    main()
