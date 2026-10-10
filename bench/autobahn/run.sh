#!/bin/bash
# Autobahn|Testsuite against websocket-bench's drivers (SPEC §10 step 4, §11.7), both directions, on a Linux host with
# Docker or podman (CI: GitHub Actions). Exits non-zero unless every case passes as expected.
# Usage: run.sh <dir with autobahnServer.kexe, autobahnClient.kexe and config/> [image]
# Environment: CONTAINER (docker | podman, default podman), NETON_IO_DRIVER (default epoll),
#   NETON_WS_COMPRESSION=1 to offer / accept permessage-deflate (then sections 12 and 13 must run, not be unimplemented),
#   AUTOBAHN_CASES to run only some cases (a comma-separated list of patterns such as 12.2.*; default all).
# Against the server, the fuzzing client runs in batches (sections 1-11, then each subsection of 12 and 13), a new process
# each: its memory grows with the cases it has run (it was killed by the kernel's OOM killer at 15.6 GB on a 16 GB runner,
# SPEC §11.10); the reports are merged. Each batch's peak RSS is printed.
set -u
cd "$1"; image=${2:-docker.io/crossbario/autobahn-testsuite}
engine=${CONTAINER:-podman}
mkdir -p reports
if [ -n "${AUTOBAHN_CASES:-}" ]; then
  python3 - "$AUTOBAHN_CASES" <<'PY'
import json, sys
cases = [c.strip() for c in sys.argv[1].split(",") if c.strip()]
for name in ("fuzzingclient", "fuzzingserver"):
    path = "config/%s.json" % name
    d = json.load(open(path)); d["cases"] = cases; json.dump(d, open(path, "w"))
PY
fi
env NETON_IO_DRIVER=${NETON_IO_DRIVER:-epoll} ./autobahnServer.kexe 127.0.0.1 9002 > server.log 2>&1 & server=$!
sleep 1
# The batches: AUTOBAHN_CASES as one, else sections 1-11 and each subsection of 12 and 13.
if [ -n "${AUTOBAHN_CASES:-}" ]; then
  batches=("$AUTOBAHN_CASES")
else
  batches=("1.*,2.*,3.*,4.*,5.*,6.*,7.*,8.*,9.*,10.*,11.*")
  for s in 12.1 12.2 12.3 12.4 12.5 13.1 13.2 13.3 13.4 13.5 13.6 13.7; do batches+=("$s.*"); done
fi
# Peak resident memory (KiB) of the fuzzing client while process $1 runs.
peak_rss() {
  local max=0 r
  while kill -0 "$1" 2>/dev/null; do
    r=$(ps -eo rss=,args= | awk '/wstest -m fuzzingclient/ && !/awk/ { s += $1 } END { print s + 0 }')
    [ "$r" -gt "$max" ] && max=$r
    sleep 2
  done
  echo "$max"
}
status=0; n=0
: > fuzz-server.log
for b in "${batches[@]}"; do
  n=$((n + 1))
  python3 - "$b" "$n" <<'PY'
import json, sys
d = json.load(open("config/fuzzingclient.json"))
d["cases"] = [c for c in sys.argv[1].split(",") if c]; d["outdir"] = "./reports/server-%s" % sys.argv[2]
json.dump(d, open("config/fuzzingclient-batch.json", "w"))
PY
  $engine run --rm --network host -v $PWD/config:/config:Z -v $PWD/reports:/reports:Z $image \
    wstest -m fuzzingclient -s /config/fuzzingclient-batch.json >> fuzz-server.log 2>&1 & fuzz=$!
  peak=$(peak_rss $fuzz)
  wait $fuzz; s=$?
  echo "fuzzing client batch $n ($b): status $s, peak RSS $((peak / 1024)) MiB"
  [ "$s" != 0 ] && status=$s
done
# One report for the server side, as a single run would write it.
python3 - $n <<'PY'
import json, os, sys
merged = {}
for i in range(1, int(sys.argv[1]) + 1):
    path = "reports/server-%d/index.json" % i
    if not os.path.exists(path):
        print("batch %d wrote no report" % i); continue
    for agent, cases in json.load(open(path)).items():
        merged.setdefault(agent, {}).update(cases)
if merged:
    os.makedirs("reports/server", exist_ok=True)
    json.dump(merged, open("reports/server/index.json", "w"))
PY
echo "fuzzing client exited with status $status"
if [ "$status" != 0 ] || [ ! -f reports/server/index.json ]; then
  # Evidence for an unexplained end: how the fuzzing client ended, the kernel's OOM or kill records, memory.
  echo "fuzzing client log tail:"; tail -3 fuzz-server.log
  (sudo -n dmesg 2>/dev/null || dmesg 2>/dev/null) | grep -i -E "oom|killed process|out of memory" | tail -5
  free -m 2>/dev/null | head -2
fi
if kill -0 $server 2>/dev/null; then kill $server; else wait $server; echo "the server exited early with status $?"; tail -20 server.log; fi
$engine run --rm --network host -v $PWD/config:/config:Z -v $PWD/reports:/reports:Z $image wstest -m fuzzingserver -s /config/fuzzingserver.json > fuzz-client.log 2>&1 & fuzz=$!
sleep 6
env NETON_IO_DRIVER=${NETON_IO_DRIVER:-epoll} ./autobahnClient.kexe ws://127.0.0.1:9101 > client.log 2>&1
kill $fuzz
compression=${NETON_WS_COMPRESSION:-0} python3 - <<'PY'
import json, collections, os, sys
compressed = os.environ["compression"] == "1"
ok_behavior = {"OK", "NON-STRICT", "INFORMATIONAL"}
ok_close = {"OK", "INFORMATIONAL"}
bad = []
for side in ("server", "client"):
    try:
        d = json.load(open("reports/%s/index.json" % side))["neton-websocket"]
    except Exception as e:
        print(side, "no report:", e); bad.append((side, "report", "missing")); continue
    print(side, len(d), "cases", dict(collections.Counter(v["behavior"] for v in d.values())),
          dict(collections.Counter(v["behaviorClose"] for v in d.values())))
    for case, v in d.items():
        b, c = v["behavior"], v["behaviorClose"]
        deflate_case = case.split(".")[0] in ("12", "13")
        if b == "UNIMPLEMENTED" and not compressed and deflate_case:
            continue                       # permessage-deflate is off in this run
        if b not in ok_behavior or (b != "UNIMPLEMENTED" and c not in ok_close):
            bad.append((side, case, b + "/" + c))
for side, case, what in bad[:50]:
    print("FAIL", side, case, what)
print(len(bad), "unexpected result(s)")
sys.exit(1 if bad else 0)
PY
