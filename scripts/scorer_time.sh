#!/usr/bin/env bash
# Scorer-exact Docker runner & paired AB/BA benchmarker for Shopping Sessions.
# Runs ON THE XEON VM (or any linux/amd64 Docker host).
#
# Key guarantees:
# - Measures exact container StartedAt -> FinishedAt via `docker inspect` (no
#   docker_run.sh exit-trap chmod contamination, no CLI client overhead).
# - Uses a fresh mktemp dir per run, verifies stdout == 0 bytes, cleans up
#   outside the timer, and optionally diffs output against a golden Parquet dir.
#
# Usage:
#   scorer_time.sh one <IMAGE> <DATA_DIR> [GOLDEN_DIR]
#   scorer_time.sh ab  <CHAMP_IMG> <CAND_IMG> <DATA_DIR> <PAIRS> [GOLDEN_DIR]
#   scorer_time.sh tier3 <IMAGE> <DATA_20M> <GOLDEN_20M> <DATA_20M_HOLD> <GOLDEN_20M_HOLD> <DATA_60M> <GOLDEN_60M>
set -euo pipefail

PY="${PY:-$HOME/buildercomp/.venv/bin/python}"
COMPARE="${COMPARE:-$HOME/buildercomp/starters/starter-duckdb/scripts/compare.py}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
PLATFORM="${PLATFORM:-linux/amd64}"

run_one() {
  local image="$1"
  local data_dir="$2"
  local golden_dir="${3:-}"
  local label="${4:-run}"

  local work
  work=$(mktemp -d /tmp/ss_run.XXXXXX)
  local out="$work/out"
  local scratch="$work/scratch"
  local cidfile="$work/cid"
  local stdout_file="$work/stdout"
  local stderr_file="$work/stderr"
  mkdir -p "$out" "$scratch"
  chmod 777 "$out" "$scratch"

  local rc=0
  sudo docker run \
    --cidfile "$cidfile" \
    --platform "$PLATFORM" \
    --network none \
    --read-only \
    --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --pids-limit 4096 \
    --cpus "$CPUS" \
    --memory "$MEM" \
    --memory-swap "$MEM" \
    --user 1000:1000 \
    -v "$data_dir":/data:ro,z \
    -v "$out":/output:z \
    -v "$scratch":/scratch:z \
    -e DATA_DIR=/data \
    -e OUTPUT_DIR=/output \
    -e SCRATCH_DIR=/scratch \
    -e HOME=/scratch \
    -e NCPU="$CPUS" \
    -e MEM_GB="${MEM%g}" \
    "$image" >"$stdout_file" 2>"$stderr_file" || rc=$?

  local cid
  cid=$(cat "$cidfile")
  local started finished oom
  started=$(sudo docker inspect -f '{{.State.StartedAt}}' "$cid")
  finished=$(sudo docker inspect -f '{{.State.FinishedAt}}' "$cid")
  oom=$(sudo docker inspect -f '{{.State.OOMKilled}}' "$cid")
  sudo docker rm -f "$cid" >/dev/null 2>&1 || true

  local stdout_bytes out_scratch_bytes
  stdout_bytes=$(wc -c < "$stdout_file" | tr -d ' ')
  out_scratch_bytes=$(sudo du -sb "$out" "$scratch" | awk '{s+=$1} END {print s}')

  local wall_ms
  wall_ms=$("$PY" - "$started" "$finished" <<'PY'
import sys
from datetime import datetime
def parse(s):
    # Trim nanoseconds to microseconds
    if "." in s:
        head, tail = s.split(".", 1)
        frac = "".join(ch for ch in tail if ch.isdigit())[:6].ljust(6, "0")
        s = f"{head}.{frac}+00:00"
    else:
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)
dt = (parse(sys.argv[2]) - parse(sys.argv[1])).total_seconds() * 1000.0
print(f"{dt:.1f}")
PY
)

  local timing_line
  timing_line=$(grep '^\[timing\]' "$stderr_file" | tail -n 1 || true)
  local internal_ms="0"
  if [[ "$timing_line" =~ total=([0-9.]+)ms ]]; then
    internal_ms="${BASH_REMATCH[1]}"
  fi
  local teardown_ms
  teardown_ms=$("$PY" -c "print(f'{float(\"$wall_ms\") - float(\"$internal_ms\"):.1f}')")

  local match_status="SKIP"
  if [ "$rc" -eq 0 ] && [ -n "$golden_dir" ] && [ -d "$golden_dir" ]; then
    if "$PY" "$COMPARE" "$out" "$golden_dir" >/dev/null 2>&1; then
      match_status="MATCH"
    else
      match_status="DIFFER"
      "$PY" "$COMPARE" "$out" "$golden_dir" >&2 || true
    fi
  fi

  echo "RESULT label=$label image=$image rc=$rc oom=$oom wall_ms=$wall_ms internal_ms=$internal_ms teardown_ms=$teardown_ms stdout_b=$stdout_bytes disk_b=$out_scratch_bytes match=$match_status | $timing_line"
  if [ "$rc" -ne 0 ]; then
    cat "$stderr_file" >&2
  fi
  sudo rm -rf "$work"

  if [ "$rc" -ne 0 ] || [ "$oom" = "true" ] || [ "$stdout_bytes" -ne 0 ] || [ "$match_status" = "DIFFER" ]; then
    return 1
  fi
}

cmd_ab() {
  local champ="$1"
  local cand="$2"
  local data_dir="$3"
  local pairs="${4:-5}"
  local golden_dir="${5:-}"

  # Warmup 1 run each (also checks candidate exactness immediately)
  echo "=== Warmup (champ + cand) ==="
  run_one "$champ" "$data_dir" "" "warmup_champ"
  run_one "$cand"  "$data_dir" "$golden_dir" "warmup_cand"

  local champ_walls=()
  local cand_walls=()
  local champ_ints=()
  local cand_ints=()

  for p in $(seq 1 "$pairs"); do
    local line_a line_b
    if (( p % 2 == 1 )); then
      line_a=$(run_one "$champ" "$data_dir" "" "p${p}_champ")
      echo "$line_a"
      line_b=$(run_one "$cand"  "$data_dir" "" "p${p}_cand")
      echo "$line_b"
    else
      line_b=$(run_one "$cand"  "$data_dir" "" "p${p}_cand")
      echo "$line_b"
      line_a=$(run_one "$champ" "$data_dir" "" "p${p}_champ")
      echo "$line_a"
    fi
    champ_walls+=("$(echo "$line_a" | sed -E 's/.*wall_ms=([0-9.]+).*/\1/')")
    cand_walls+=("$(echo "$line_b" | sed -E 's/.*wall_ms=([0-9.]+).*/\1/')")
    champ_ints+=("$(echo "$line_a" | sed -E 's/.*internal_ms=([0-9.]+).*/\1/')")
    cand_ints+=("$(echo "$line_b" | sed -E 's/.*internal_ms=([0-9.]+).*/\1/')")
  done

  "$PY" - "${champ_walls[*]}" "${cand_walls[*]}" "${champ_ints[*]}" "${cand_ints[*]}" <<'PY'
import statistics, sys, random

cw = [float(x) for x in sys.argv[1].split()]
nw = [float(x) for x in sys.argv[2].split()]
ci = [float(x) for x in sys.argv[3].split()]
ni = [float(x) for x in sys.argv[4].split()]

diffs_w = [n - c for c, n in zip(cw, nw)]
diffs_i = [n - c for c, n in zip(ci, ni)]
med_c, med_n = statistics.median(cw), statistics.median(nw)
med_ci, med_ni = statistics.median(ci), statistics.median(ni)
pct_w = (med_n - med_c) / med_c * 100.0
pct_i = (med_ni - med_ci) / med_ci * 100.0

# Bootstrap 95% CI on paired median diff
rng = random.Random(42)
boots = []
k = len(diffs_w)
for _ in range(2000):
    sample = [diffs_w[rng.randrange(k)] for _ in range(k)]
    boots.append(statistics.median(sample))
boots.sort()
lo, hi = boots[int(0.025 * len(boots))], boots[int(0.975 * len(boots))]
verdict = "WIN" if hi < 0 else ("LOSS" if lo > 0 else "INCONCLUSIVE")
print(f"AB_SUMMARY verdict={verdict} pairs={k} "
      f"champ_wall_med={med_c:.1f}ms cand_wall_med={med_n:.1f}ms delta_wall={med_n-med_c:+.1f}ms ({pct_w:+.2f}%) ci95=[{lo:+.1f},{hi:+.1f}]ms "
      f"champ_int_med={med_ci:.1f}ms cand_int_med={med_ni:.1f}ms delta_int={med_ni-med_ci:+.1f}ms ({pct_i:+.2f}%)")
PY
}

cmd_tier3() {
  local image="$1"
  local d20="$2" g20="$3"
  local d20h="$4" g20h="$5"
  local d60="$6" g60="$7"

  echo "=== Tier 3.1: 20M primary (3x warm + exact MATCH) ==="
  run_one "$image" "$d20" "$g20" "t3_20m_1"
  run_one "$image" "$d20" ""     "t3_20m_2"
  run_one "$image" "$d20" ""     "t3_20m_3"

  echo "=== Tier 3.2: 20M holdout seed 777 (3x warm + exact MATCH) ==="
  run_one "$image" "$d20h" "$g20h" "t3_20mh_1"
  run_one "$image" "$d20h" ""      "t3_20mh_2"
  run_one "$image" "$d20h" ""      "t3_20mh_3"

  echo "=== Tier 3.3: 60M cold page-cache (1x + exact MATCH) ==="
  sync && echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
  run_one "$image" "$d60" "$g60" "t3_60m_cold"

  echo "=== Tier 3.4: 60M warm (7x idle-machine median, exact final protocol) ==="
  local w60=()
  for i in 1 2 3 4 5 6 7; do
    local line
    line=$(run_one "$image" "$d60" "" "t3_60m_warm_${i}")
    echo "$line"
    w60+=("$(echo "$line" | sed -E 's/.*wall_ms=([0-9.]+).*/\1/')")
  done
  "$PY" - "${w60[*]}" <<'PY'
import statistics, sys
vals = [float(x) for x in sys.argv[1].split()]
print(f"TIER3_60M_SUMMARY median={statistics.median(vals):.1f}ms min={min(vals):.1f}ms max={max(vals):.1f}ms runs={[round(v,1) for v in vals]}")
PY
}

case "${1:-}" in
  one)   shift; run_one "$@" ;;
  ab)    shift; cmd_ab "$@" ;;
  tier3) shift; cmd_tier3 "$@" ;;
  *) echo "Usage: $0 {one|ab|tier3} ..." >&2; exit 2 ;;
esac
