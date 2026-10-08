#!/bin/bash
# Autobahn|Testsuite against websocket-bench's drivers (SPEC §10 step 4, §11.7), both directions, on a Linux host with
# Docker or podman (CI: GitHub Actions). Exits non-zero unless every case passes as expected.
# Usage: run.sh <dir with autobahnServer.kexe, autobahnClient.kexe and config/> [image]
# Environment: CONTAINER (docker | podman, default podman), NETON_IO_DRIVER (default epoll),
#   NETON_WS_COMPRESSION=1 to offer / accept permessage-deflate (then sections 12 and 13 must run, not be unimplemented),
#   AUTOBAHN_CASES to run only some cases (a comma-separated list of patterns such as 12.2.*; default all).
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
$engine run --rm --network host -v $PWD/config:/config:Z -v $PWD/reports:/reports:Z $image wstest -m fuzzingclient -s /config/fuzzingclient.json > fuzz-server.log 2>&1
echo "fuzzing client exited with status $?"
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
