#!/usr/bin/env bash
# Two joins over a1/joins/{customers,orders}.csv on the cluster master.
#   LABEL=part4   tags the log files and YARN job names, so re-runs stay apart
set -euo pipefail

# This term's cluster image keeps the streaming JAR in /usr/lib/hadoop; older images used
# /usr/lib/hadoop-mapreduce. Test each candidate directly: an unmatched glob stays literal and
# fails -f, which avoids `ls` returning non-zero on a missing path and killing us under `set -e`.
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
TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG_REDUCE="run_join_reduce${SUFFIX}_${TS}.log"
LOG_MAP="run_join_map${SUFFIX}_${TS}.log"

echo "== reduce-side join =="
hdfs dfs -rm -r -f a1/out_join_reduce
# Part 4.1a: 20 reducers, so the skewed key's straggler becomes visible in the Job History UI.
hadoop jar "$STREAMING" -D mapreduce.job.name="a1-join-reduce-side${SUFFIX}" \
  -D mapreduce.job.reduces=20 \
  -files reduce_side_mapper.py,reduce_side_reducer.py \
  -mapper reduce_side_mapper.py -reducer reduce_side_reducer.py \
  -input a1/joins/customers.csv -input a1/joins/orders.csv -output a1/out_join_reduce 2>&1 | tee "$LOG_REDUCE"

echo "== map-side (broadcast) join =="
hdfs dfs -get -f a1/joins/customers.csv customers.csv          # the small side travels with the job
hdfs dfs -rm -r -f a1/out_join_map
hadoop jar "$STREAMING" -D mapreduce.job.name="a1-join-map-side${SUFFIX}" -D mapreduce.job.reduces=0 \
  -files map_side_mapper.py,customers.csv \
  -mapper map_side_mapper.py \
  -input a1/joins/orders.csv -output a1/out_join_map 2>&1 | tee "$LOG_MAP"

# The map-side job emits one row per order, so roll it up locally to get the same
# four columns the reduce-side job prints. Both files then feed crosscheck.py.
echo "== collecting both outputs as TSV for the cross-check =="
hdfs dfs -cat a1/out_join_reduce/part-* | LC_ALL=C sort -t $'\t' -k1,1 > "out_join_reduce${SUFFIX}.tsv"
hdfs dfs -cat a1/out_join_map/part-* | awk -F'\t' '
  { n[$1]++; s[$1] += $2 }
  END { for (k in n) { split(k, a, "|"); printf "%s\t%s\t%d\t%.2f\n", a[1], a[2], n[k], s[k] } }' \
  | LC_ALL=C sort -t $'\t' -k1,1 > "out_join_map_totals${SUFFIX}.tsv"
wc -l "out_join_reduce${SUFFIX}.tsv" "out_join_map_totals${SUFFIX}.tsv"

echo "---- compare the counters in the two logs: 'Reduce input groups', 'Reduce shuffle bytes', 'Map output bytes' ----"
grep -E "Reduce input groups|Reduce shuffle bytes|Map output bytes|Map input records" "$LOG_REDUCE" "$LOG_MAP" || true
echo "---- logs: $LOG_REDUCE  $LOG_MAP ----"
