#!/bin/sh
# Test 4 - Chaos
# Repeatedly: write a key (retrying through re-elections), then SIGKILL a
# random node and restart it shortly after. At the end, every acknowledged
# write must still be readable. No committed write may be lost.
cd "$(dirname "$0")/.." || exit 1
. tests/lib.sh

DURATION="${CHAOS_SECONDS:-20}"

cluster_init
start_all
wait_for_leader || fail "no leader elected"

# Commit a write to any current leader, tolerating in-flight elections.
commit_key() { # commit_key <n>
    n="$1"
    tries=0
    while [ "$tries" -lt 40 ]; do
        for id in 1 2 3; do
            eval "pid=\$pid_$id"
            [ -z "$pid" ] && continue
            kill -0 "$pid" 2>/dev/null || continue
            out=$(kv "$id" "SET k$n v$n" 2>/dev/null)
            [ "$out" = "OK" ] && return 0
        done
        tries=$((tries + 1))
        sleep 0.1
    done
    return 1
}

start=$(date +%s)
n=0
acked=0
while [ $(( $(date +%s) - start )) -lt "$DURATION" ]; do
    if commit_key "$n"; then
        acked="$n"
    else
        fail "could not commit k$n within timeout"
    fi
    n=$((n + 1))

    # Crash a random node, then bring it back after a short delay.
    victim=$(( (n % 3) + 1 ))
    kill_node "$victim"
    sleep 0.2
    start_node "$victim"
done

echo "acknowledged writes: k0..k$acked"
# Let the last-restarted node rejoin so the cluster is healthy for reads.
sleep 1
wait_for_leader || fail "no leader at end of chaos run"

i=0
while [ "$i" -le "$acked" ]; do
    out=""
    tries=0
    while [ "$tries" -lt 30 ]; do
        out=$(kv 1 "GET k$i" 2>/dev/null)
        [ -n "$out" ] && [ "$out" != "ERR no leader" ] && break
        tries=$((tries + 1)); sleep 0.1
    done
    [ "$out" = "v$i" ] || fail "lost committed write k$i (got '$out')"
    i=$((i + 1))
done
pass "all $((acked + 1)) acknowledged writes survived chaos"

stop_all
echo "TEST 4 PASSED"
