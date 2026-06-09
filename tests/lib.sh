#!/bin/sh
# Shared helpers for the raft-kv test scripts. POSIX sh.

# Binary under test (override with BIN=./raft-kv-tsan for sanitizer runs).
BIN="${BIN:-./raft-kv}"
CLIENT="${CLIENT:-./raft-client}"
DATADIR="testdata"

P1=7001
P2=7002
P3=7003

# pid_<id> holds each node's process id; empty when stopped.
pid_1=""
pid_2=""
pid_3=""

cluster_init() {
    rm -rf "$DATADIR"
    mkdir -p "$DATADIR"
}

start_node() {
    id="$1"
    case "$id" in
        1) port=$P1; peers="$P2,$P3" ;;
        2) port=$P2; peers="$P1,$P3" ;;
        3) port=$P3; peers="$P1,$P2" ;;
    esac
    $BIN --id "$id" --port "$port" --peers "$peers" --datadir "$DATADIR" \
        >>"$DATADIR/n$id.log" 2>&1 &
    eval "pid_$id=$!"
}

start_all() {
    start_node 1
    start_node 2
    start_node 3
}

kill_node() { # hard crash, like the README's SIGKILL tests
    id="$1"
    eval "pid=\$pid_$id"
    if [ -n "$pid" ]; then
        kill -9 "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
        eval "pid_$id="
    fi
}

stop_all() {
    for id in 1 2 3; do
        eval "pid=\$pid_$id"
        [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
        eval "pid_$id="
    done
    wait 2>/dev/null
}

port_for() {
    case "$1" in
        1) echo $P1 ;;
        2) echo $P2 ;;
        3) echo $P3 ;;
    esac
}

# Run a command against a running node, following redirects to the leader.
kv() { # kv <id> "<COMMAND>"
    $CLIENT --port "$(port_for "$1")" -c "$2"
}

# Identify the current leader's node id by the highest term that any node
# logged becoming leader for. Echoes the id, or nothing if none yet.
find_leader_id() {
    best_id=""
    best_term=-1
    for id in 1 2 3; do
        eval "pid=\$pid_$id"
        [ -z "$pid" ] && continue
        kill -0 "$pid" 2>/dev/null || continue
        term=$(grep 'became LEADER for term' "$DATADIR/n$id.log" 2>/dev/null \
               | tail -1 | sed 's/.*term //')
        [ -z "$term" ] && continue
        if [ "$term" -gt "$best_term" ]; then
            best_term="$term"
            best_id="$id"
        fi
    done
    echo "$best_id"
}

# Wait until the cluster can commit a write (a leader exists). Returns 0/1.
wait_for_leader() {
    i=0
    while [ "$i" -lt 50 ]; do
        for id in 1 2 3; do
            eval "pid=\$pid_$id"
            [ -z "$pid" ] && continue
            kill -0 "$pid" 2>/dev/null || continue
            out=$(kv "$id" "SET __probe__ 1" 2>/dev/null)
            if [ "$out" = "OK" ]; then
                return 0
            fi
        done
        i=$((i + 1))
        sleep 0.1
    done
    return 1
}

# Write keys k<lo>..k<hi> with value v<n>, through any live node.
write_range() { # write_range <id> <lo> <hi>
    id="$1"; lo="$2"; hi="$3"
    n="$lo"
    while [ "$n" -le "$hi" ]; do
        out=$(kv "$id" "SET k$n v$n")
        if [ "$out" != "OK" ]; then
            echo "FAIL: write k$n returned '$out'"
            return 1
        fi
        n=$((n + 1))
    done
    return 0
}

# Verify keys k<lo>..k<hi> each read back v<n>.
verify_range() { # verify_range <id> <lo> <hi>
    id="$1"; lo="$2"; hi="$3"
    n="$lo"
    while [ "$n" -le "$hi" ]; do
        out=$(kv "$id" "GET k$n")
        if [ "$out" != "v$n" ]; then
            echo "FAIL: GET k$n returned '$out', expected 'v$n'"
            return 1
        fi
        n=$((n + 1))
    done
    return 0
}

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; stop_all; exit 1; }
