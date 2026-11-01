# ADSP 32030 Assignment 1 — Parts 1–4 answers

Cluster: Dataproc 2.2 (Debian 12), 1 master + 2 workers, **e2-standard-2**.
`n2-standard-2` had no capacity in any `us-central1` zone, so I substituted E2 — same
2 vCPU / 8 GB, so block counts, split counts and every counter below are unaffected.

Created **2026-10-05 18:03:44 UTC** · deleted **2026-10-05 20:33:22 UTC** · uptime **2 h 29 min**

Topology: `10.128.0.3` master/NameNode · `10.128.0.4` = `w-0` · `10.128.0.2` = `w-1` (the one stopped in 3.2).

---

## Part 1a — staging and block layout

`corpus.txt` is **300,000,008 bytes** and HDFS stored it as **3 blocks** at **replication 2**,
with `dfs.blocksize = 134217728` and `dfs.replication = 2`:

| block | length | replicas |
|---|---|---|
| `blk_1073741825_1001` | 134,217,728 | `10.128.0.4`, `10.128.0.2` |
| `blk_1073741826_1002` | 134,217,728 | `10.128.0.4`, `10.128.0.2` |
| `blk_1073741827_1003` | 31,564,552 | `10.128.0.2`, `10.128.0.4` |

`fsck` reported `HEALTHY`, 2 data-nodes, 1 rack, 0 under-replicated, 0 corrupt.
**Every block sits on both DataNodes** — with two workers and replication 2 there is nowhere
else a replica could go. That one fact drives the whole of Part 3.1.

Ground truth for the corpus: `wc -w` = **71,323,584** words.

---

## Part 1b — word count, no combiner (`job_…_0001`)

| counter | value |
|---|---|
| Map input records | 7,132,619 |
| Map output records | **71,323,584** |
| Map output bytes | 442,647,176 |
| Reduce input groups | **88** |
| Reduce shuffle bytes | **585,294,506** |
| Reduce input records | 71,323,584 |
| Reduce output records | 88 |
| HDFS bytes read / written | 300,033,739 / 1,110 |
| Spilled records | 142,647,168 |
| Elapsed | **3 min 27 s** |

`Map output records` equals `wc -w` exactly, and `Reduce input groups` is 88 — the generator's
whole vocabulary. Both jobs' outputs summed to 71,323,584 over 88 words.

---

## Part 1c — the combiner

| counter | no combiner | with combiner |
|---|---|---|
| Map output records | 71,323,584 | 71,323,584 |
| Map output **bytes** | 442,647,176 | 442,647,176 |
| Combine input records | 0 | **71,323,584** |
| Combine output records | 0 | **792** |
| **Reduce shuffle bytes** | **585,294,506** | **10,953** |
| Reduce input records | 71,323,584 | 792 |
| Spilled records | 142,647,168 | 1,584 |
| Elapsed | 3 min 27 s | **2 min 43 s** |
| Avg map / shuffle / merge / reduce | 35 s / 11 s / 17 s / 40 s | 40 s / 11 s / **0 s** / **1 s** |

**Shuffle fell by a factor of 53,437.** `792 = 88 × 9` exactly — 88 possible keys, one combiner
flush per map task, 9 tasks. Note `Map output bytes` is *identical* in both runs: the mapper
emitted the same 442 MB either way; the combiner changed only what crossed the network. The cost
shows up as maps getting slower (35 s → 40 s) and the benefit as merge and reduce collapsing
(17 s → 0 s, 40 s → 1 s), for a net saving of 44 seconds.

**Why the combiner is legal here, and when it is not.** The combiner is legal for word count
because the reducer's operation is integer addition, which is associative and commutative, so
summing partial sums per map task and then summing those gives exactly the same answer as summing
every `1` in one place — and because the reducer's output type (word, count) matches its input
type, it can safely be fed its own output. A combiner is not legal when the aggregate is not
decomposable that way — a mean, a median, or a distinct count, anything that must see every value
for a key at once — because a partial mean of partial means is not the mean, and a reducer whose
output shape differs from its input shape cannot run twice over the same stream.

---

## Part 2.1 — reduce-side join (`job_…_0003`)

`Map input records` 2,001,002 → `Map output records` **2,001,000**; the difference of 2 is exactly
the two header rows the mapper skips. **`Reduce input groups` = 1000** — one group per
`customer_id`, which *is* the join. `Reduce output records` = 1000. Elapsed **1 min 34 s**.

## Part 2.2 — map-side (broadcast) join, and the comparison

| | reduce-side | map-side |
|---|---|---|
| Map output bytes | 23,649,383 | — (no shuffle) |
| **Reduce shuffle bytes** | **27,651,563** | **0** |
| Launched reduce tasks | 4 | **0** |
| Spilled records | 4,002,000 | **0** |
| CPU time | 48,900 ms | **21,530 ms** |
| HDFS bytes written | 32,355 | 48,099,892 |
| Counters reported | 56 | **33** |
| Elapsed | 1 min 34 s | **1 min 05 s** |

**The reduce-side join moved 27,651,563 bytes (26.4 MiB) across the network; the map-side join
moved none.** The absent counters are the evidence: the map-side job reports 33 counters instead
of 56 because the entire Reduce family does not exist. `Spilled Records = 0` likewise — no shuffle
means no sort, no spill to local disk, no merge, where the reduce-side job spilled every record
twice.

**What makes the map-side version possible** is that `customers.csv` is small: 1,000 rows, about
30 KB, so a complete copy fits in every mapper's memory. `-files` ships it through the distributed
cache once per node, each mapper builds a dict at start-up, and every order is resolved in place.
Against ~30 KB broadcast per node, that is roughly 460× less data on the wire. The reduce-side
join has no such option — it cannot know which records share a key until the data has been
partitioned by key, so every order must travel to whichever reducer owns its customer.

Two honest caveats: the map-side job writes **more** to HDFS (48 MB vs 32 KB) because it emits one
row per order rather than one per customer, so it trades network for storage; and the per-customer
aggregation still has to happen somewhere — in my case a local roll-up afterwards.

**Reconciliation.** Both joins produced 1,000 rows, 2,000,000 orders and **$91,138,637.40**, are
byte-identical to each other, and match an independent pandas merge (`crosscheck.py --engine pandas`).

---

## Part 3.1 — where the map tasks ran, and why there are 9 of them

| | no combiner | with combiner |
|---|---|---|
| number of splits | **9** | 9 |
| Launched map tasks | **11** | 10 |
| Killed map tasks | 2 | 1 |
| **Data-local map tasks** | **11** | **10** |
| Rack-local map tasks | *counter absent* | *counter absent* |

**Every map attempt in both jobs was data-local; the rack-local counter is absent, which in Hadoop
means zero.** That was easy to satisfy: with two workers at replication 2, both DataNodes hold all
three blocks, so any split could be scheduled locally on either machine.

**The file is 3 blocks but the job ran 9 splits.** Three separate reasons, in order of size:

1. **A block is a unit of storage; a split is a unit of work.** Hadoop Streaming uses the old
   `mapred` API, where `goalSize = totalSize / mapreduce.job.maps` and
   `splitSize = max(minSize, min(goalSize, blockSize))`. On this cluster `mapreduce.job.maps` is
   effectively 9, so the split count tracks that hint, not the block count. My own three jobs prove
   it: `corpus.txt` (300,000,008 B) → 9 splits, `orders.csv` (58,550,736 B) → 9 splits, and
   `orders.csv` + `customers.csv` → 10 splits (9 for orders, 1 for the 30 KB customers file). A
   300 MB file and a 56 MB file producing the same split count cannot be block-driven.
2. **Splits do not respect record boundaries, and I measured the cost.** A split ends at the first
   newline *after* its byte range, so the reader must read ahead into the next split. Comparing the
   9-split run against a forced 3-split run of the same file:
   `File Input Format → Bytes Read` was **300,032,776** vs **300,008,200** — a difference of
   **24,576 bytes = 6 × 4096**, exactly `io.file.buffer.size` per removed split boundary.
3. **Speculative execution launches duplicates.** `mapreduce.map.speculative` defaults to true, so
   `Launched map tasks` counts attempts, not splits: 9 splits + 2 speculative duplicates = 11
   launched, and the losers appear as `Killed map tasks = 2`. The combiner run was 9 + 1 = 10.

**Confirming experiment.** Re-running with
`split.minsize=134217728` and `map.speculative=false` gave exactly **3 splits, 3 launched map
tasks, 3 data-local, no killed tasks** — same file, same cluster, same output, differing only in
config. Bigger splits were also *worse*: wall time rose from 3 min 27 s to **3 min 56 s**, spilled
records from 142,647,168 to **206,464,912**, and local-disk reads from 585 MB to **1,109 MB**,
because each map task had to sort three times as many records through the same fixed buffer.

---

## Part 3.2 — stopping a worker mid-job

I started the no-combiner word count (`job_…_0006`) and stopped `w-1` (`10.128.0.2`) from Cloud
Shell at **19:11:18 UTC**.

| time (UTC) | event | evidence |
|---|---|---|
| 19:10:33 | job submitted, 9 splits | job log |
| **19:11:18** | **`stop-worker` on `w-1`** | `date -u` |
| 19:11:25 | **progress goes backwards, 19% → 11%** | job log |
| 19:13:05 | YARN already reports `Total Nodes:1` | `failure_timeline.txt` |
| 19:13:05 | HDFS: `Live 2`, `Under-replicated 0`, `HEALTHY` | same |
| 19:17–19:20 | under-replicated creeps 1 → 2 → 3, denominator 24 → 26 | same |
| **19:20:49** | **job completes successfully** | job log |
| **19:22:19** | NameNode marks `w-1` dead; under-replicated jumps to **26 (100 %)** | same |
| 19:22–19:28 | stuck at 26; `Blocks queued for replication: 0`, `Avg repl 1.0` | `fsck` |
| **19:28:10** | **`start-worker`** | `date -u` |
| **≤ 19:30:34** | `Live 2`, under-replicated **0**, `Avg repl 2.0`, `Total Nodes:2` | `healing_timeline.txt` |

**What YARN did.** `Killed map tasks` rose from 2 (clean run) to **4**, and a counter that is
absent in a healthy run appeared: **`Other local map tasks = 2`**, with `Data-local` falling from
11 to 9. Seven seconds after the kill, reported progress dropped from 19 % back to 11 % — completed
work on the dead node was discarded and re-queued. Notably YARN did **not** go through a `LOST`
state: `gcloud compute instances stop` is a graceful shutdown, so the NodeManager received SIGTERM
and deregistered cleanly. The 10-minute liveness timeout only applies to a node that vanishes
without warning.

**What the NameNode reported, and how long it took.** For roughly ten minutes after a machine had
physically stopped, HDFS reported `Live datanodes (2)` and `Under-replicated blocks: 0`. Detection
happened between my 19:20:55 and 19:22:19 samples — **9 min 37 s to 11 min 1 s after the kill**,
bracketing the expected `2 × dfs.namenode.heartbeat.recheck-interval + 10 × heartbeat ≈ 10.5 min`.
The gradual rise to 1, 2, then 3 before that is *not* the corpus blocks: the denominator grows
24 → 26, so those are new output blocks the running job wrote, which landed with a single replica
because the NameNode still believed `w-1` was available. Once it declared the node dead, all **26**
blocks lost a replica at once. It then **stayed** at 26 with `Blocks queued for replication: 0` —
HDFS was not merely slow to heal, it was not trying, because each replica needs a distinct
DataNode and only one remained. Throughout, `fsck` reported **`HEALTHY`**: under-replicated is not
corrupt, and no data was lost even at zero redundancy.

**How much longer the job took.** **10 min 2 s**, against a clean baseline of **3 min 27 s** — 2.9×.
The damage was confined to the reduce phase: average map, shuffle and merge times all *fell*, while
**average reduce time rose from 40 s to 1 min 32 s** as reducers retried shuffle fetches against a
machine that no longer answered.

**Recovery.** Full recovery within **46 s to 2 min 24 s** of `start-worker`, and — worth being
precise — this was **re-registration, not re-replication**. `Blocks queued for replication` was 0 at
every sample, because stopping a Compute Engine instance preserves its persistent disk: `w-1` came
back still holding all 26 replicas and simply reported them. Had the node been deleted, HDFS would
have had to stream ~300 MB from `w-0`.

---

## Part 3.3 — 128 MB blocks, 3 replicas, and why mine used 2

My cluster: `dfs.blocksize = 134217728` (128 MiB), `dfs.replication = 2`, 2 DataNodes, 1 rack.

**Why 128 MB.** The NameNode holds metadata for every block in RAM, roughly 150 bytes each, so
large blocks keep the metadata budget small for a given volume of data. Large blocks also make
reads effectively sequential: at ~100 MB/s a 128 MB block streams in about a second while a disk
seek costs ~10 ms, so transfer dominates seek. A block is also the default sizing hint for a map
task, so much smaller blocks would mean many more containers each paying seconds of JVM start-up
for proportionally less work.

*Wrong for:* a corpus of millions of small files, where each 4 KB file still consumes a block
object in NameNode memory and yields a map task that finishes before it warms up — the small-files
problem. Also wrong for low-latency point lookups, where you want kilobytes and most of a 128 MB
block is wasted I/O. My own forced-3-split experiment showed the other edge of this: oversized
units of work left cores idle *and* increased spilling, raising wall time 14 % and local disk reads
89 %.

**Why 3 replicas.** Three copies survive two simultaneous node failures, give the scheduler three
candidate nodes when placing a data-local task, and spread read load for hot blocks — at 3× the
storage cost.

*Wrong for:* cold archival data, where erasure coding reaches comparable durability at ~1.5×
overhead instead of 3×. Also wrong for short-lived derived data that is cheaper to recompute than
to store three times.

**Why mine used 2.** HDFS places each replica on a distinct DataNode and this cluster has only two
workers, so replication 3 could never be satisfied — every block would sit permanently
under-replicated and the NameNode would warn forever. Dataproc sizes the default to the cluster it
created. Part 3.2 showed the consequence directly: at replication 2, losing one of two DataNodes
left exactly one copy of all 26 blocks and **no redundancy at all** until the worker returned, with
HDFS unable to re-replicate in the meantime. The job survived; a second failure in that window
would have lost data.

---

## Part 4.1 — the straggler

Re-running the reduce-side join with `-D mapreduce.job.reduces=20` (`job_…_0007`): 20 reduce tasks,
`Reduce input groups` still 1000, totals unchanged, but wall time rose from 1 min 34 s to
**2 min 42 s** and total reduce-task time from 55,063 ms to **223,467 ms** — twenty containers each
paying full JVM start-up for a twentieth of 27 MB.

**The skew is real.** Summing each reducer's output partition:

| | reduce input records | customers |
|---|---|---|
| reducer 00 (holds customer 1) | **196,694** | 51 |
| 2nd heaviest (04) | 106,718 | 56 |
| median | 93,999 | — |
| lightest (13) | 85,506 | 45 |

**2.30× heaviest to lightest.** Reducer 00 carries the same ~51 customers as everyone else;
**customer 1's 101,743 orders — 5.09 % of all 2,000,000 — more than double its load on their own.**
Strip that one key and reducer 00 holds 94,951 records, dead on the median.

I do want to note that the skew does not appear in elapsed time. Reduce task times ranged 7–15 s, and
`r_000000` — the one with 2.3× the data — took **8 s, among the fastest**. The reason is in the
component columns: **reduce time was 0–1 s for every task**, while shuffle took 6–13 s. 196,694
records of `O,12.34` is about 2.5 MB, which a Python reducer sums in well under a second; the task
is dominated by fetching ten map outputs, a fixed cost that varies with container placement rather
than key distribution. At this data size perhaps the job is overhead-bound, not data-bound.

---

## Part 4.2 — salting

The mapper appends `#0`…`#9` to `customer_id`: each order picks one salt at random, and **every
customers row is emitted once per salt** so each of the ten groups still finds the name. The
step-1 reducer emits a partial `(customer_id, name, n, total)` per salt with the salt stripped, and
a second MapReduce step sums the partials back to one row per customer. Partial totals carry six
decimals, not two — rounding ten partials to cents before summing would drift.

`Map output records` went 2,001,000 → **2,010,000**, exactly the 9,000 extra rows from replicating
1,000 customers across ten salts instead of one. Step 1 produced **10,000** partials from
**10,000** reduce input groups; step 2 collapsed them to **1,000**.

| | unsalted, 20 reducers | salted, 20 reducers |
|---|---|---|
| Heaviest reducer | 196,694 | **111,023** |
| Lightest reducer | 85,506 | 89,994 |
| **Imbalance** | **2.30×** | **1.23×** |
| **Slowest reduce task** | **15 s** | **15 s** |
| Shuffle bytes | 27,652,583 | 31,873,742 + 364,339 |
| MapReduce jobs | 1 | **2** |
| **Total wall time** | **2 min 42 s** | **4 min 39 s** |
| Totals | $91,138,637.40 | **$91,138,637.40** |

Customer 1's ten salts landed on ten different reducers (0, 1, 2, 13, 14, 15, 16, 17, 18, 19).
The output is **byte-identical** to the Part 2 join and reconciles against the pandas cross-check.

**Salting halved the data imbalance and changed the slowest reduce task by zero seconds.** Load and
elapsed time are uncorrelated here: the heaviest reducer (01, 111,023 records) finished in 7 s while
a below-average one (09, 91,405) took 15 s. The 7 s / 15 s split is container placement, not data.
So the fix was correct and cost **72 % more wall time** — a second MapReduce pass and 16 % more
shuffle to cure a straggler that was never costing anything. Skew correction pays for itself when
the reduce phase dominates; at 27 MB it does not.

---

## Part 4.3 — beating the cluster

`time python3 join_local.py` on the master: **2.21 s**, pandas, **232 MB peak RSS**, output
byte-identical to the Part 2 join and reconciling to $91,138,637.40.

| job | wall time | vs local |
|---|---|---|
| `join_local.py` | **2.2 s** | — |
| Map-side broadcast join | 1 min 05 s | 29× |
| Reduce-side join, 3 reducers | 1 min 34 s | 43× |
| Reduce-side join, 20 reducers | 2 min 42 s | 73× |
| Salted join (2 jobs) | 4 min 39 s | **126×** |

**Why one process wins at two million rows.** Almost all of Hadoop's time here is fixed cost — JVM
start-up for ten mappers and twenty reducers, container allocation, HDFS round trips, sorting and
spilling 27 MB through local disk, HTTP shuffle fetches — and none of it scales down with small
data. My own counters show it: reduce *compute* was about one second per task while shuffle was
6–13 seconds. The real work is seconds; the machinery around it is minutes.

**Where it flips.** I measured pandas at **232 MB peak for 58.5 MB of input, a ~4× memory-to-data
ratio**, which on this 8 GB machine caps a single process near **1.5–2 GB of CSV — roughly 50–70
million orders, about 30× this dataset**. Past that, pandas swaps and the cluster wins, not because
it is faster but because it is the only thing that fits. That ratio is a property of the
implementation rather than the problem, though: the streaming standard-library path in the same
script peaked at **15 MB**, 0.26× the input, and would stay ahead far longer. The honest crossover
is where data exceeds one machine's RAM *at whatever ratio your code needs*.
