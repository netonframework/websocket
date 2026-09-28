#!/bin/bash
# Ir/message of a WebSocket echo server under ws-bench (5 s and 15 s runs): (Ir15-Ir5)/(msg15-msg5).
set -u
label=$1; srv=$2; out=/root/bench/ws/cg/$label; mkdir -p $out
for secs in 5 15; do
  if [ $srv = neton ]; then cmd="/root/bench/autobahn/autobahnServer.kexe 127.0.0.1 9611"
  else cmd="/root/bench/ws/ws-bench/target/release/ws-bench server 127.0.0.1:9611"; fi
  env NETON_IO_DRIVER=epoll taskset -c 1 valgrind --tool=cachegrind --cache-sim=no --cachegrind-out-file=$out/$srv-$secs.out $cmd > $out/$srv-$secs.log 2>&1 &
  pid=$!; sleep 3
  taskset -c 2,3 /root/bench/ws/ws-bench/target/release/ws-bench client ws://127.0.0.1:9611/ 50 $secs 64 2 > $out/$srv-$secs.load
  kill -INT $pid; sleep 2; kill $pid 2>/dev/null; wait $pid 2>/dev/null
done
python3 - $out $srv <<'PY'
import sys,re
out,srv=sys.argv[1],sys.argv[2]
ir=lambda f:[int(l.split()[1]) for l in open(f) if l.startswith('summary:')][0]
msgs=lambda f:int(re.search(r'msgs=(\d+)',open(f).read()).group(1))
i5,i15=ir(f'{out}/{srv}-5.out'),ir(f'{out}/{srv}-15.out'); m5,m15=msgs(f'{out}/{srv}-5.load'),msgs(f'{out}/{srv}-15.load')
print(f'{srv}: msgs5={m5} msgs15={m15} Ir/msg={(i15-i5)/(m15-m5):.0f}')
PY
