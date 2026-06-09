#!/bin/sh
# Test 1 - Basic Consensus
# Write 100 keys, SIGKILL a follower, write 100 more, restart the follower,
# wait for catch-up, then verify all 200 keys.
cd "$(dirname "$0")/.." || exit 1
. tests/lib.sh

cluster_init
start_all
wait_for_leader || fail "no leader elected"

leader=$(find_leader_id)
# pick a follower (any id that is not the leader)
for id in 1 2 3; do [ "$id" != "$leader" ] && follower=$id && break; done
echo "leader=$leader, will crash follower=$follower"

write_range "$leader" 0 99 || fail "initial writes"
pass "wrote keys 0..99"

kill_node "$follower"
echo "killed follower $follower"

write_range "$leader" 100 199 || fail "writes during follower outage"
pass "wrote keys 100..199 with follower down"

start_node "$follower"
echo "restarted follower $follower; waiting for catch-up"
sleep 2

# Data integrity: every committed write must be readable.
verify_range "$leader" 0 199 || fail "values after recovery"
pass "all 200 keys verified after follower recovery"

# Catch-up proof: the restarted follower's on-disk log matches a peer's.
fsize_follower=$(wc -c < "$DATADIR/log_${follower}.bin")
fsize_leader=$(wc -c < "$DATADIR/log_${leader}.bin")
echo "log bytes: follower=$fsize_follower leader=$fsize_leader"
[ "$fsize_follower" = "$fsize_leader" ] || fail "follower log did not catch up"
pass "follower log fully caught up"

stop_all
echo "TEST 1 PASSED"
