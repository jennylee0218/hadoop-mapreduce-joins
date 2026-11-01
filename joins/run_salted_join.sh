#!/usr/bin/env bash
# Part 4.2: the salted reduce-side join, in two steps.
#   step 1  salt the hot key, replicate customers across every salt -> per-salt partials
#   step 2  sum the partials back to one row per customer
# Env: LABEL=part4 tags logs and YARN job names.  REDUCES=20 reducers for step 1.
set -euo pipefail

if [[ -z "${STREAMING:-}" ]]; then
  for candidate in /usr/lib/hadoop/hadoop-streaming.jar \
                   /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
                   /usr/lib/hadoop/hadoop-streaming-*.jar \
                   /usr/lib/hadoop-mapreduce/hadoop-streaming-*.jar; do
    if [[ -f "$candidate" ]]; then STREAMING=$candidate; break; fi
  done
fi
if [[ ! -f "${STREAMING:-}" ]]; then
  echo "No hadoop-streaming JAR found. Run: find /usr/lib -name 'hadoop-streaming*.jar'" >&2
  echo "then: export STREAMING=<the path it prints>" >&2
  exit 1
fi
echo "streaming JAR: $STREAMING"

SUFFIX=${LABEL:+_${LABEL}}
REDUCES=${REDUCES:-20}
TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG1="run_salted_step1${SUFFIX}_${TS}.log"
LOG2="run_salted_step2${SUFFIX}_${TS}.log"

echo "== step 1: salted join, $REDUCES reducers =="
hdfs dfs -rm -r -f a1/out_salted_partials
hadoop jar "$STREAMING" -D mapreduce.job.name="a1-join-salted-step1${SUFFIX}" \
  -D mapreduce.job.reduces="$REDUCES" \
  -files salted_mapper.py,salted_reducer.py \
  -mapper salted_mapper.py -reducer salted_reducer.py \
  -input a1/joins/customers.csv -input a1/joins/orders.csv \
  -output a1/out_salted_partials 2>&1 | tee "$LOG1"

# Step 2 is tiny -- at most 10 partials per customer, so ~10,000 rows. One reducer is plenty.
echo "== step 2: merge the per-salt partials =="
hdfs dfs -rm -r -f a1/out_join_salted
hadoop jar "$STREAMING" -D mapreduce.job.name="a1-join-salted-step2${SUFFIX}" \
  -D mapreduce.job.reduces=1 \
  -files merge_reducer.py \
  -mapper cat -reducer merge_reducer.py \
  -input a1/out_salted_partials -output a1/out_join_salted 2>&1 | tee "$LOG2"

echo "== collecting output =="
hdfs dfs -cat a1/out_join_salted/part-* | LC_ALL=C sort -t $'\t' -k1,1 > "out_join_salted${SUFFIX}.tsv"
wc -l "out_join_salted${SUFFIX}.tsv"
echo "partial rows written by step 1: $(hdfs dfs -cat a1/out_salted_partials/part-* | wc -l)"
echo "---- logs: $LOG1  $LOG2 ----"
