#!/usr/bin/env bash
# Records exactly which raw reports support the published issue-0011 tables.
#
# `bench/results/` is gitignored and always will be — the raw JSON is large and
# machine-specific. But "trust me, I ran it" is not evidence, and a table whose
# inputs cannot be identified is not reproducible. So the *manifest* is checked
# in: every report's label, its content hash, the code that produced it, and the
# headline numbers a reader would want to challenge.
#
# Usage: bench/manifest-issue-0011.sh > bench/manifests/issue-0011.json
set -euo pipefail
cd "$(dirname "$0")/.."

sha=$(git rev-parse HEAD)
dirty=$(git status --porcelain --untracked-files=no | wc -l)

echo "{"
echo "  \"issue\": \"0011\","
echo "  \"git_sha\": \"$sha\","
echo "  \"git_dirty\": $([ "$dirty" -eq 0 ] && echo false || echo true),"
echo "  \"host\": {"
echo "    \"cores\": $(nproc),"
echo "    \"mem_gb\": $(awk '/MemTotal/ {printf "%.0f", $2/1048576}' /proc/meminfo)"
echo "  },"
echo "  \"reports\": ["

first=1
for f in bench/results/catalog-{seed,explain,read,admission}-*p3*.json; do
  [ -e "$f" ] || continue
  [ $first -eq 1 ] || echo ","
  first=0
  hash=$(sha256sum "$f" | cut -d' ' -f1)
  # Pull the few fields a reader would actually challenge, so the manifest is
  # readable on its own rather than a list of opaque hashes.
  summary=$(python3 - "$f" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
keep = ["label", "workload", "mode", "achieved_qps", "p50_ms", "p99_ms", "timed_out",
        "result_rows", "total_parts", "live_parts", "logical_tables", "namespaces",
        "tenant_parts_p50", "tenant_parts_p99", "verdict_passes", "achieved_headroom"]
out = {k: d[k] for k in keep if k in d}
if "saturation" in d and d["saturation"]:
    s = d["saturation"]
    out["saturation"] = s.get("class")
    out["pg_cpu_cores_used"] = s.get("pg_cpu_cores_used")
    out["driver_cpu_cores_used"] = s.get("driver_cpu_cores_used")
    out["cache_hit_ratio"] = s.get("cache_hit_ratio")
print(json.dumps(out, indent=6)[1:-1].rstrip())
PY
)
  printf '    {\n      "file": "%s",\n      "sha256": "%s",%s\n    }' \
    "$(basename "$f")" "$hash" "$summary"
done

echo
echo "  ]"
echo "}"
