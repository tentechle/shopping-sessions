#!/bin/sh
# Run the static binary the way the scorer runs the image, without Docker:
# empty root (only /pipeline), /data read-only, /output and /scratch writable,
# 64 MB /tmp, no network, 24 GB memory without swap, 4096 tasks, 8 CPUs.
# Usage: scripts/sim_run.sh BINARY DATA_DIR OUTPUT_DIR
set -eu
BIN=$(realpath "$1"); DATA=$(realpath "$2"); mkdir -p "$3"; OUT=$(realpath "$3")
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
CPUS="${CPUS:-0-7}"
exec systemd-run --user --scope -q -p MemoryMax=24G -p MemorySwapMax=0 -p TasksMax=4096 \
  taskset -c "$CPUS" bwrap --unshare-all --die-with-parent \
    --ro-bind "$BIN" /pipeline --ro-bind "$DATA" /data --bind "$OUT" /output --bind "$SCRATCH" /scratch \
    --tmpfs /tmp --proc /proc --dev /dev --clearenv \
    --setenv DATA_DIR /data --setenv OUTPUT_DIR /output --setenv SCRATCH_DIR /scratch --setenv HOME /scratch \
    --setenv NCPU 8 --setenv MEM_GB 24 ${PIPE_DEBUG:+--setenv PIPE_DEBUG 1} \
    /pipeline
