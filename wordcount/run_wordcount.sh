#!/usr/bin/env bash
# Run word count as a Hadoop Streaming job on the cluster master.
#   ./run_wordcount.sh            -> with combiner   (output a1/out_wc_combiner)
#   ./run_wordcount.sh nocombiner -> without         (output a1/out_wc_plain)
# Optional env vars:
#   LABEL=part3   tags the log file and the YARN job name, so re-runs stay apart
#   EXTRA="-D mapreduce.map.speculative=false"   extra -D flags passed to hadoop jar
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

TAG=${1:-combiner}
SUFFIX=${LABEL:+_${LABEL}}
EXTRA=${EXTRA:-}                      # intentionally unquoted below so it word-splits into flags
LOG="run_${TAG}${SUFFIX}_$(date -u +%Y%m%dT%H%M%SZ).log"
IN=a1/input/corpus.txt

# Everything in one array, so there is no empty-array expansion to trip over.
ARGS=(-D "mapreduce.job.name=a1-wordcount-${TAG}${SUFFIX}"
      -files mapper.py,reducer.py
      -mapper mapper.py -reducer reducer.py)
if [[ "$TAG" == "nocombiner" ]]; then
  OUT=a1/out_wc_plain
else
  OUT=a1/out_wc_combiner
  ARGS+=(-combiner reducer.py)
fi

hdfs dfs -rm -r -f "$OUT"
hadoop jar "$STREAMING" $EXTRA "${ARGS[@]}" -input "$IN" -output "$OUT" 2>&1 | tee "$LOG"
echo "---- log written to $LOG ----"
echo "---- top 10 words ----"
# '|| true' because head closes the pipe early, which pipefail would otherwise call a failure
hdfs dfs -cat "$OUT/part-*" | sort -t $'\t' -k2,2nr | head -10 || true
