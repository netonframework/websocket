#!/bin/bash
# Autobahn|Testsuite against websocket-bench's drivers (SPEC §10 step 4), on a Linux host with podman.
# Usage: run.sh <dir with autobahnServer.kexe, autobahnClient.kexe and config/>  [image]
# The image is crossbario/autobahn-testsuite (from a registry mirror when Docker Hub is unreachable).
set -u
cd "$1"; image=${2:-docker.io/crossbario/autobahn-testsuite}
mkdir -p reports
env NETON_IO_DRIVER=${NETON_IO_DRIVER:-epoll} ./autobahnServer.kexe 127.0.0.1 9002 > server.log 2>&1 & server=$!
sleep 1
podman run --rm --network host -v $PWD/config:/config:Z -v $PWD/reports:/reports:Z $image wstest -m fuzzingclient -s /config/fuzzingclient.json > fuzz-server.log 2>&1
kill $server
podman run --rm --network host -v $PWD/config:/config:Z -v $PWD/reports:/reports:Z $image wstest -m fuzzingserver -s /config/fuzzingserver.json > fuzz-client.log 2>&1 & fuzz=$!
sleep 6
env NETON_IO_DRIVER=${NETON_IO_DRIVER:-epoll} ./autobahnClient.kexe ws://127.0.0.1:9101 > client.log 2>&1
kill $fuzz
python3 - <<PY
import json,collections
for side in ("server","client"):
    d=json.load(open("reports/%s/index.json"%side))["neton-websocket"]
    print(side, dict(collections.Counter(v["behavior"] for v in d.values())), dict(collections.Counter(v["behaviorClose"] for v in d.values())))
PY
