#!/usr/bin/env python3
"""ADSP 32030 · Assignment 1 data generator (deterministic: everyone gets the same files).

    python3 make_data.py            # writes ./data/corpus.txt (~300 MB), customers.csv (1,000 rows), orders.csv (2,000,000 rows)
    python3 make_data.py --small    # 10x smaller, for a quick local test

Then stage it in HDFS:
    hdfs dfs -mkdir -p a1/input a1/joins
    hdfs dfs -put data/corpus.txt a1/input/
    hdfs dfs -put data/customers.csv data/orders.csv a1/joins/
"""
import argparse, csv, os, random

SEED = 32030
WORDS = ("the of and to in a is that for it as was with be by on not he this are or his from at which but have an had they you were "
         "their one all we can her has there been if more when will would who so no she cloud data pipeline shuffle reducer mapper "
         "block replica namenode datanode hadoop spark stream event agent vector graph model query index storage compute network "
         "chicago autumn tuesday scalable system latency throughput partition key value join broadcast locality failure retry").split()
# Zipf-like weights so a few words dominate — this is what makes the combiner matter
WEIGHTS = [1.0 / (i + 1) ** 0.9 for i in range(len(WORDS))]

def corpus(path, target_bytes, rng):
    with open(path, "w", encoding="utf-8") as f:
        written = 0
        while written < target_bytes:
            n = rng.randint(6, 14)
            line = " ".join(rng.choices(WORDS, weights=WEIGHTS, k=n)) + "\n"
            f.write(line); written += len(line)
    return written

def customers(path, n, rng):
    first = ["Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Linus", "Margaret", "Ken", "Dennis", "Radia", "Tim", "Frances", "Guido", "Yukihiro", "Anita"]
    last = ["Lovelace", "Hopper", "Turing", "Dijkstra", "Liskov", "Knuth", "Torvalds", "Hamilton", "Thompson", "Ritchie", "Perlman", "Berners-Lee", "Allen", "van Rossum", "Matsumoto", "Borg"]
    states = ["IL", "NY", "CA", "TX", "WA", "MA", "GA", "CO", "OH", "MI"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["customer_id", "name", "state", "segment"])
        for cid in range(1, n + 1):
            w.writerow([cid, f"{rng.choice(first)} {rng.choice(last)}", rng.choice(states), rng.choice(["consumer", "smb", "enterprise"])])

def orders(path, n, n_customers, rng):
    # heavily skewed: customer 1 gets ~5% of all orders (you will meet this skew again in Week 2)
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["order_id", "customer_id", "order_date", "amount"])
        for oid in range(1, n + 1):
            cid = 1 if rng.random() < 0.05 else rng.randint(1, n_customers)
            day = rng.randint(1, 365)
            w.writerow([oid, cid, f"2025-{(day - 1) // 31 + 1:02d}-{(day - 1) % 28 + 1:02d}", f"{rng.lognormvariate(3.5, 0.8):.2f}"])

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--small", action="store_true"); ap.add_argument("--out", default="data")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    scale = 0.1 if a.small else 1.0
    rng = random.Random(SEED)
    b = corpus(f"{a.out}/corpus.txt", int(300_000_000 * scale), rng); print(f"corpus.txt      {b/1e6:8.1f} MB")
    customers(f"{a.out}/customers.csv", 1000, rng); print("customers.csv    1,000 rows")
    n = int(2_000_000 * scale); orders(f"{a.out}/orders.csv", n, 1000, rng); print(f"orders.csv   {n:>9,} rows")
