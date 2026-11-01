#!/usr/bin/env bash
# Create / connect to / delete the Assignment 1 Dataproc cluster. Run from Cloud Shell.
#   ./cluster.sh create   ./cluster.sh ssh      ./cluster.sh where    ./cluster.sh delete
#   ./cluster.sh push     ./cluster.sh pull     ./cluster.sh stop-worker|start-worker
#
# You normally need to set nothing. `create` walks every zone in the region until one has
# capacity, and ssh / stop-worker / start-worker then look up the zone the cluster is really in,
# so there is no ZONE to remember or re-export in a new Cloud Shell tab.
#   export REGION=us-west1                          # different region
#   export ZONE=us-central1-b                        # pin one zone instead of walking
#   export ZONES=us-central1-b,us-central1-c         # walk just these
#   export MACHINE=e2-standard-2                     # when n2 is out of capacity everywhere
#          (e2-standard-2 is the same 2 vCPU / 8 GB, and cheaper — nothing in the
#           assignment depends on the machine family; say so in your writeup)
set -euo pipefail
NAME=${CLUSTER:-adsp32030-a1}
REGION=${REGION:-us-central1}
ZONE=${ZONE:-}                      # empty = discover it from the cluster / walk all zones
MACHINE=${MACHINE:-n2-standard-2}   # the brief's type; override when a region is out of capacity
echo "[cluster.sh] $NAME in $REGION / ${ZONE:-zone auto-discovered}" >&2

# The zone the cluster is actually in — needed because Auto Zone may not pick what you asked for.
cluster_zone() {
  gcloud dataproc clusters describe "$NAME" --region "$REGION" \
    --format="value(config.gceClusterConfig.zoneUri.basename())"
}
resolve_zone() {
  if [[ -n "$ZONE" ]]; then echo "$ZONE"; else cluster_zone; fi
}

# State of an existing cluster of this name, empty if there is none.
cluster_state() {
  gcloud dataproc clusters list --region "$REGION" \
    --filter="clusterName=$NAME" --format="value(status.state)" 2>/dev/null
}

case "${1:-}" in
  create)
    # Zones fill up, and Dataproc's auto-zone is not capacity-aware, so walk candidates until
    # one works. Override the list with ZONES=us-central1-b,us-central1-c or pin one with ZONE=.
    if [[ -n "${ZONES:-}" ]]; then
      CANDIDATES=$(tr ',' ' ' <<< "$ZONES")
    elif [[ -n "$ZONE" ]]; then
      CANDIDATES=$ZONE
    else
      CANDIDATES=$(gcloud compute zones list --filter="region:($REGION) AND status=UP" \
                     --format="value(name)" 2>/dev/null | sort | tr '\n' ' ')
      [[ -n "$CANDIDATES" ]] || CANDIDATES="${REGION}-a ${REGION}-b ${REGION}-c ${REGION}-f"
    fi
    echo "[cluster.sh] $MACHINE; will try zones: $CANDIDATES" >&2

    STATE=$(cluster_state)
    if [[ "$STATE" == "RUNNING" ]]; then
      echo "$NAME is already RUNNING in $(cluster_zone). Use it, or './cluster.sh delete' first." >&2
      exit 1
    elif [[ -n "$STATE" && "$STATE" != ERROR* ]]; then
      echo "$NAME exists in state $STATE — wait for it to settle, or './cluster.sh delete'." >&2
      exit 1
    fi

    gcloud services enable dataproc.googleapis.com compute.googleapis.com
    for Z in $CANDIDATES; do
      echo "[cluster.sh] ==== trying $Z ====" >&2
      # A failed create leaves the cluster in ERROR holding the name; clear it before retrying.
      [[ -n "$(cluster_state)" ]] && gcloud dataproc clusters delete "$NAME" --region "$REGION" --quiet
      if gcloud dataproc clusters create "$NAME" --region "$REGION" --zone "$Z" \
           --master-machine-type "$MACHINE" --master-boot-disk-size 50 \
           --num-workers 2 --worker-machine-type "$MACHINE" --worker-boot-disk-size 50 \
           --image-version 2.2-debian12 --enable-component-gateway --max-idle 1h; then
        echo
        echo "Cluster $NAME is up in $Z. Web UIs: console -> Dataproc -> $NAME -> Web Interfaces"
        echo "RUN THIS NOW:   export ZONE=$Z"
        exit 0
      fi
      echo "[cluster.sh] $Z had no capacity; trying the next zone" >&2
    done
    echo "No zone in $REGION had $MACHINE capacity. Two things to try:" >&2
    echo "  export MACHINE=e2-standard-2   # same 2 vCPU / 8 GB, far more available, cheaper" >&2
    echo "  export REGION=us-west1         # or a different region" >&2
    exit 1 ;;
  where)
    gcloud dataproc clusters list --region "$REGION" \
      --format="table(clusterName,status.state,config.gceClusterConfig.zoneUri.basename():label=ZONE,status.stateStartTime:label=SINCE)" ;;
  ssh)          gcloud compute ssh "${NAME}-m" --zone "$(resolve_zone)" ;;
  push)         gcloud compute scp --recurse "${2:-$HOME/a1_starter}" "${NAME}-m:~" --zone "$(resolve_zone)" ;;
  pull)         gcloud compute scp --recurse "${NAME}-m:~/a1_starter/${2:-submit}" . --zone "$(resolve_zone)" ;;
  stop-worker)  gcloud compute instances stop  "${NAME}-w-1" --zone "$(resolve_zone)" ;;   # Part 3.2: kill a DataNode mid-job
  start-worker) gcloud compute instances start "${NAME}-w-1" --zone "$(resolve_zone)" ;;
  delete)       gcloud dataproc clusters delete "$NAME" --region "$REGION" --quiet ;;
  *) echo "usage: $0 create|ssh|push [dir]|pull [dir]|where|stop-worker|start-worker|delete"; exit 1 ;;
esac
