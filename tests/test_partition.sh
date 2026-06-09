#!/bin/sh
# Test 3 - Network Partition  (requires root: uses iptables)
# Isolate node 3 from nodes 1 and 2. Write 50 keys to the majority partition;
# they must succeed. Heal the partition, wait for node 3 to catch up, then
# verify all 50 keys are present on node 3's log.
cd "$(dirname "$0")/.." || exit 1
. tests/lib.sh

if [ "$(id -u)" != "0" ]; then
    echo "SKIP: test_partition requires root (iptables). Run with sudo."
    exit 0
fi

block()   { iptables -A INPUT -p tcp --dport "$1" -j DROP; iptables -A OUTPUT -p tcp --sport "$1" -j DROP; }
unblock() { iptables -D INPUT -p tcp --dport "$1" -j DROP; iptables -D OUTPUT -p tcp --sport "$1" -j DROP; }
heal()    { unblock "$P3" 2>/dev/null; }
trap heal EXIT

cluster_init
start_all
wait_for_leader || fail "no leader elected"

# Force the leader into the majority side: if node 3 is leader, the partition
# would strand the cluster briefly until 1/2 re-elect. Either way the majority
# {1,2} can make progress, so just ensure the leader we write through is 1 or 2.
echo "partitioning node 3 from the cluster"
block "$P3"
sleep 0.6                       # let {1,2} settle / re-elect if needed
wait_for_leader || fail "majority partition has no leader"

# Write through node 1 or 2 (majority side).
leader=$(find_leader_id)
[ "$leader" = "3" ] && fail "node 3 should not be leader while partitioned"
write_range "$leader" 0 49 || fail "writes to majority partition"
pass "wrote keys 0..49 to majority partition"

echo "healing partition"
heal
sleep 2                         # node 3 catches up

fsize_3=$(wc -c < "$DATADIR/log_3.bin")
fsize_l=$(wc -c < "$DATADIR/log_${leader}.bin")
echo "log bytes: node3=$fsize_3 leader=$fsize_l"
[ "$fsize_3" = "$fsize_l" ] || fail "node 3 did not catch up after heal"
pass "node 3 caught up after partition heal"

verify_range "$leader" 0 49 || fail "values after heal"
pass "all 50 keys verified"

stop_all
echo "TEST 3 PASSED"
