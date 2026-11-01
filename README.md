# Hadoop MapReduce: combiners, joins and data skew

Word count and three join strategies written as **Hadoop Streaming** jobs in Python, run on a 3-node
Google Cloud Dataproc cluster (1 master + 2 workers) over a 300 MB corpus and a 2-million-row orders table.
Every claim is backed by a job counter, an HDFS report or a log in [`logs/`](logs/).

Full write-up with every number: **[RESULTS.md](RESULTS.md)**

## What I built
- **Word count**, with and without a combiner — the mapper/reducer pair plus a combiner that is legal because
  addition is associative and commutative.
- **Reduce-side join** (`orders ⋈ customers`, tagged values, one group per `customer_id`).
- **Map-side broadcast join** — the small table shipped to every mapper through the distributed cache; no shuffle at all.
- **Salted join** for a hot key: orders get a random salt `#0…#9`, the small side is replicated to every salt, and a
  second MapReduce pass sums the partials back to one row per customer.
- **Failure experiment** — stopping a DataNode mid-job and watching HDFS re-replicate and YARN re-run tasks.
- A **pytest suite** that runs every mapper and reducer on small hand-checked samples, with a stand-in for Hadoop's sort.

## Key results
| experiment | result |
|---|---|
| Combiner on word count | shuffle **585,294,506 → 10,953 bytes (53,437× less)**; `792 = 88 words × 9 map tasks` |
| Map-side vs reduce-side join | **27.6 MB → 0 bytes** shuffled, 1 min 34 s → 1 min 05 s, 33 counters instead of 56 (no Reduce family) |
| Skew (customer 1 = 5.09 % of orders) | heaviest reducer **2.30×** the lightest |
| Salting, 10 salts | imbalance **2.30× → 1.23×**, totals byte-identical ($91,138,637.40) — but **72 % more wall time** |
| Single-process pandas | **2.2 s** vs 65 s–4 min 39 s on the cluster; crossover estimated at ~50–70 M orders on an 8 GB box |

The most useful finding: at this scale the job is **overhead-bound, not data-bound**. Reduce compute was ~1 s per task
against 6–13 s of shuffle fetch, so fixing the skew was correct and still made the job slower.

## Layout
```
wordcount/   mapper.py · reducer.py · run_wordcount.sh
joins/       reduce_side_* · map_side_mapper.py · salted_* · merge_reducer.py · join_local.py · crosscheck.py · run_*.sh
tests/       pytest for every mapper/reducer on hand-checked samples
logs/        job counters, fsck/dfsadmin reports, per-reducer load, join outputs
screenshots/ YARN job history pages (combiner vs no combiner, map tasks, reducers before/after salting)
```

## Run it
```bash
pip install -r requirements.txt
pytest -q                     # 18 tests, no cluster needed
```
On a Dataproc cluster: `python3 make_data.py`, put the data in HDFS, then `wordcount/run_wordcount.sh` and
`joins/run_joins.sh` / `joins/run_salted_join.sh`.

## Stack
Hadoop 3 (Dataproc 2.2) · HDFS · YARN · Hadoop Streaming · Python · pytest · Google Cloud

---
Course work for *ADSP 32030 Scalable Data Systems*, University of Chicago (Fall 2026).
`make_data.py` and `cluster.sh` were provided by the course; the jobs, tests, experiments and write-up are mine.
