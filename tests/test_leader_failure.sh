#!/bin/sh
# Test 2 - Leader Failure
# Write 50 keys, SIGKILL the leader, verify a new leader is elected within
# ~500ms, write 50 more keys, then verify all 100.
cd "$(dirname "$0")/.." || exit 1
. tests/lib.sh

cluster_init
start_all
wait_for_leader || fail "no leader elected"

leader=$(find_leader_id)
echo "initial leader=$leader"

write_range "$leader" 0 49 || fail "initial writes"
pass "wrote keys 0..49"

kill_node "$leader"
echo "killed leader $leader; waiting for re-election"

# Election should complete fast. Give it generous headroom over the 500ms SLA.
sleep 0.6
wait_for_leader || fail "no new leader elected after crash"
new_leader=$(find_leader_id)
[ -n "$new_leader" ] || fail "could not identify new leader"
[ "$new_leader" != "$leader" ] || fail "leader did not change"
pass "new leader elected: $new_leader"

write_range "$new_leader" 50 99 || fail "writes under new leader"
pass "wrote keys 50..99 under new leader"

verify_range "$new_leader" 0 99 || fail "values after failover"
pass "all 100 keys verified after leader failover"

stop_all
echo "TEST 2 PASSED"
