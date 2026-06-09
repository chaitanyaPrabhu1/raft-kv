# raft-kv

Fault-tolerant distributed key-value store built on the Raft consensus algorithm in pure C — leader election, log replication, and crash recovery over raw POSIX sockets with no external dependencies.

---

## What This Is

A cluster of three C processes that together behave as one reliable key-value store. A client sends `SET x 5` or `GET x` to any node. The cluster guarantees that once a write is acknowledged, it survives any single node failure — including a hard crash with `SIGKILL` at the worst possible moment.

The implementation follows the Raft paper (Ongaro & Ousterhout, 2014) closely. Every design decision is traceable to a specific guarantee in the paper. Where the paper is ambiguous, this document explains the choice made and why.

---

## Guarantees

**What this system guarantees:**

- A write acknowledged to the client has been persisted to a majority of nodes and will survive any single node failure.
- After a leader crash, the cluster elects a new leader within 500ms and resumes serving requests.
- A node that rejoins after a crash replays the log and catches up to the current state without manual intervention.
- No committed entry is ever lost or overwritten.

**What this system does not guarantee:**

- Linearizable reads. `GET` is served from the leader's local state. A leader that has been partitioned but not yet deposed may serve stale reads. A strict implementation would route reads through the log; that is noted as a known limitation.
- Log compaction. The log grows unboundedly. Snapshotting (Section 7 of the paper) is not implemented.
- Byzantine fault tolerance. This system tolerates crash failures only — a node that lies or behaves arbitrarily is outside the threat model.
- Cross-datacenter deployment. Election timeouts are tuned for localhost latency. Wide-area networks require different timer values.

---

## Quick Start

**Requirements:** GCC or Clang, Linux (uses `timerfd_create`, `O_DSYNC`), pthreads.

**Build:**

```sh
make
```

**Start a 3-node cluster:**

```sh
# Terminal 1
./raft-kv --id 1 --port 7001 --peers 7002,7003

# Terminal 2
./raft-kv --id 2 --port 7002 --peers 7001,7003

# Terminal 3
./raft-kv --id 3 --port 7003 --peers 7001,7002
```

**Connect a client:**

```sh
./raft-client --port 7001

> SET name chaitanya
OK
> GET name
chaitanya
> DEL name
OK
```

If you connect to a follower, it transparently redirects you to the current leader.

**Run the test suite:**

```sh
make test
```

**Run under sanitizers:**

```sh
make test-tsan    # ThreadSanitizer
make test-asan    # AddressSanitizer + UBSan
```

---

## Architecture

```
┌─────────┐     TCP      ┌──────────────────────────────────────────┐
│  Client │ ──────────►  │                  Node                    │
└─────────┘             │                                          │
                        │  ┌────────────┐    ┌──────────────────┐  │
                        │  │ Main Thread│    │ Election Timer   │  │
                        │  │ (listener) │    │ Thread           │  │
                        │  └─────┬──────┘    └────────┬─────────┘  │
                        │        │                    │             │
                        │        ▼                    ▼             │
                        │  ┌──────────────────────────────────┐    │
                        │  │         Raft State Machine        │    │
                        │  │  (mutex-protected shared state)   │    │
                        │  └────┬──────────────────┬──────────┘    │
                        │       │                  │                │
                        │       ▼                  ▼                │
                        │  ┌─────────┐      ┌───────────┐          │
                        │  │  Log    │      │ KV Store  │          │
                        │  │ (disk)  │      │ (memory)  │          │
                        │  └─────────┘      └───────────┘          │
                        │                                          │
                        │  ┌────────────┐    ┌──────────────────┐  │
                        │  │ Heartbeat  │    │  Apply Thread    │  │
                        │  │ Thread     │    │  (log → KV)      │  │
                        │  └────────────┘    └──────────────────┘  │
                        └──────────────────────────────────────────┘
```

Each node runs four threads:

| Thread | Role |
|---|---|
| Main | Accepts TCP connections, dispatches RPCs |
| Election timer | Fires when no heartbeat received; triggers candidate state |
| Heartbeat | Leader only; sends `AppendEntries` every 100ms |
| Apply | Watches `commit_index`; applies committed entries to KV store |

All shared node state is protected by a single `pthread_mutex_t`. Correctness before optimization.

---

## Consensus: How It Works

### Leader Election

Every follower runs an election timer reset to a random value between 150ms and 300ms. The timer resets on every valid heartbeat received from the current leader.

When the timer fires without a heartbeat, the follower:

1. Increments its current term
2. Transitions to candidate state
3. Votes for itself
4. Sends `RequestVote` RPC to all peers

A peer grants its vote if and only if:
- The candidate's term is at least as large as the peer's current term
- The candidate's log is at least as up-to-date as the peer's log (compared by last log term, then last log index)
- The peer has not already voted in this term

A candidate that receives votes from a majority (2 of 3) becomes leader immediately. It begins sending heartbeats to suppress further elections.

**Why random timeouts?** If all followers had the same timeout, they would all call elections simultaneously on every leader failure, splitting votes indefinitely. Randomization statistically ensures one node fires first and wins before others wake up.

### Log Replication

The log is append-only. Every entry carries an index, a term, and a command. The in-memory KV state is the log replayed from index 1 — the log is the truth, not the hash table.

On a client write:

1. Leader appends the entry to its own log (disk write with `O_DSYNC`)
2. Leader sends `AppendEntries` RPC to all followers
3. Each follower appends the entry to its log and replies success
4. Once a majority acknowledges, leader advances `commit_index`
5. Leader applies the entry to the KV store
6. Leader responds to the client: `OK`

The client receives `OK` only after step 6. An entry is durable the moment the majority acknowledge it in step 3, because those writes used `O_DSYNC`.

### Follower Catch-Up

The leader tracks `nextIndex[i]` per follower — the next log index to send to follower i.

When a follower rejects an `AppendEntries` (log inconsistency at the prefix check), the leader decrements `nextIndex[i]` and retries. This walks back until the follower's log matches the leader's at the prefix point. The leader then sends all entries from that point forward. The follower overwrites any conflicting entries.

This is safe because conflicting entries on a follower were never committed — if they had been, a majority would hold them, and a node missing them could not have won election.

---

## Persistence

Three things are written to disk before responding to any RPC:

| Data | File | Why |
|---|---|---|
| Current term | `meta.bin` | Prevents voting twice in the same term across a restart |
| Voted-for | `meta.bin` | Prevents granting two votes in the same term |
| Log entries | `log.bin` | Source of truth for all committed state |

Both files are opened with `O_DSYNC`. This flag instructs the kernel to flush data to the physical storage device before the `write` call returns. Without it, a power failure after `write` returns but before the kernel flushes could silently lose data that the system believed was persisted.

This is the same guarantee PostgreSQL uses for WAL writes.

**Startup sequence:**

1. Read `meta.bin` → restore `current_term`, `voted_for`
2. Read `log.bin` → restore full log array
3. Replay all entries up to `commit_index` → rebuild KV store state
4. Begin participating in the cluster

---

## Wire Protocol

Raw TCP, binary encoding. No HTTP, no gRPC, no Protobuf.

### Framing

Every message begins with a 4-byte header:

```
┌──────────┬─────────────────────────┐
│  1 byte  │        3 bytes          │
│   type   │   payload length (BE)   │
└──────────┴─────────────────────────┘
```

Followed immediately by the payload. All multi-byte integers are big-endian. Strings are length-prefixed: 2 bytes for length, then the UTF-8 bytes. No null terminators. No padding.

### Message Types

| Byte | Name | Direction |
|---|---|---|
| `0x01` | RequestVote | Candidate → Peers |
| `0x02` | RequestVoteResponse | Peer → Candidate |
| `0x03` | AppendEntries | Leader → Followers |
| `0x04` | AppendEntriesResponse | Follower → Leader |
| `0x05` | ClientRequest | Client → Any node |
| `0x06` | ClientResponse | Node → Client |
| `0x07` | Redirect | Follower → Client |

### RequestVote Payload

```
┌──────────────┬─────────────────┬──────────────────┬──────────────────┐
│   4 bytes    │     4 bytes     │     8 bytes      │     4 bytes      │
│     term     │  candidate_id   │  last_log_index  │  last_log_term   │
└──────────────┴─────────────────┴──────────────────┴──────────────────┘
```

### AppendEntries Payload

```
┌────────┬──────────┬──────────────┬───────────┬──────────────┬─────────────────┐
│ 4 byte │  4 byte  │   8 bytes    │  4 bytes  │   8 bytes    │  variable       │
│  term  │ leader   │ prev_log     │ prev_log  │   commit     │  entries[]      │
│        │   id     │   index      │   term    │   index      │                 │
└────────┴──────────┴──────────────┴───────────┴──────────────┴─────────────────┘
```

Each entry in `entries[]`:

```
┌──────────┬───────────┬──────────────────────────────────┐
│  8 bytes │  4 bytes  │          variable                │
│  index   │   term    │   command (length-prefixed str)  │
└──────────┴───────────┴──────────────────────────────────┘
```

### Partial Reads

TCP is a stream protocol. A single `read` call may return fewer bytes than requested. All receive operations use `recv_all`:

```c
ssize_t recv_all(int fd, void *buf, size_t n) {
    size_t received = 0;
    while (received < n) {
        ssize_t r = read(fd, (char *)buf + received, n - received);
        if (r <= 0) return r;
        received += r;
    }
    return (ssize_t)received;
}
```

Every network read in the codebase goes through `recv_all`. There are no exceptions.

---

## KV Store

Open-addressing hash table with linear probing. Fixed-size keys and values (64 bytes each). The hash table is rebuilt from the log on every startup — it is purely derived state.

**Supported commands:**

| Command | Replication | Description |
|---|---|---|
| `SET key value` | Yes — through Raft | Write a value |
| `DEL key` | Yes — through Raft | Delete a key |
| `GET key` | No — local read | Read a value |

`GET` is served from the leader's local state without going through the log. This is fast but means reads are not strictly linearizable. A future implementation would route reads through a no-op log entry to guarantee the leader is current before responding.

---

## Test Suite

The test suite is the most important part of the project. It is the proof that the implementation is correct, not merely functional.

### Test 1 — Basic Consensus

Start 3 nodes. Write 100 keys. Kill one follower with `SIGKILL`. Write 100 more keys. Restart the follower. Wait for catch-up. Read all 200 keys and verify every value.

```sh
./tests/test_basic.sh
```

### Test 2 — Leader Failure

Start 3 nodes. Write 50 keys. Send `SIGKILL` to the leader. Wait 500ms. Verify a new leader is elected. Write 50 more keys. Read all 100 keys and verify correctness.

```sh
./tests/test_leader_failure.sh
```

### Test 3 — Network Partition

Use `iptables` to isolate node 3 from nodes 1 and 2. Write 50 keys to the majority partition — they should succeed. Restore the network. Wait for node 3 to catch up. Read all 50 keys from node 3 and verify correctness.

```sh
sudo ./tests/test_partition.sh
```

### Test 4 — Chaos

For 30 seconds: write a key, randomly `SIGKILL` a random node, restart it after 200ms, repeat. At the end, read every key ever written and verify no committed write was lost.

```sh
./tests/test_chaos.sh
```

### Running Under Sanitizers

```sh
# Data race detection — any reported race is a correctness bug
make test-tsan

# Memory errors and undefined behaviour
make test-asan
```

A clean run under ThreadSanitizer is a stronger correctness argument than any code review.

---

## Directory Structure

```
raft-kv/
├── src/
│   ├── main.c          # Entry point, argument parsing, node startup
│   ├── raft.c          # Core consensus state machine
│   ├── raft.h
│   ├── log.c           # Persistent log — O_DSYNC append, replay on startup
│   ├── log.h
│   ├── net.c           # TCP server, recv_all, send_all, message framing
│   ├── net.h
│   ├── kv.c            # Hash table — open addressing, linear probing
│   ├── kv.h
│   ├── timer.c         # Election timer (timerfd), heartbeat timer
│   ├── timer.h
│   └── client.c        # CLI client with automatic leader redirect
├── tests/
│   ├── test_basic.sh
│   ├── test_leader_failure.sh
│   ├── test_partition.sh
│   └── test_chaos.sh
├── Makefile
└── README.md
```

---

## Design Decisions

**Why `O_DSYNC` instead of `fsync`?**

`fsync` is a separate syscall issued after the write. There is a window between `write` returning and `fsync` completing where a crash loses data. `O_DSYNC` closes that window — the `write` itself does not return until the data hits stable storage. The cost is higher write latency, which is acceptable because log writes are not on the critical path for reads.

**Why a single mutex over reader-writer locks?**

Reader-writer locks optimize for read-heavy workloads where many concurrent readers are common. In this implementation, the consensus state machine transitions frequently — every heartbeat, every RPC, every timer fire touches shared state. The contention profile does not favor rwlocks, and the added complexity introduces more surface area for bugs. A single mutex is correct and fast enough for a 3-node cluster. This is the same approach SQLite uses for its WAL locking.

**Why random election timeouts between 150–300ms?**

The paper recommends timeouts be an order of magnitude larger than one-way network latency. On localhost, one-way latency is under 1ms. 150ms is generous. The 150ms lower bound prevents spurious elections on a loaded system. The 300ms upper bound keeps failover fast. The random range statistically prevents split votes — the probability of two nodes firing within 1ms of each other is low, and even if they do, the next round resolves it.

**Why a binary protocol over HTTP or text?**

A text protocol (like Redis RESP) is easier to debug with netcat but wastes bytes on framing characters and requires a parser for variable-length tokens. A binary protocol with a fixed 4-byte header is parsed in two `read` calls regardless of payload size. More importantly, implementing the wire format from scratch is the point — it forces a precise understanding of what TCP actually delivers and why `recv_all` is necessary.

**Why pure C over C++?**

The system primitives this project uses — `pthreads`, `timerfd`, POSIX sockets, `O_DSYNC`, `pread`/`pwrite` — are C APIs. Using them directly in C means every allocation, every error path, and every synchronization point is explicit and visible. There is nowhere to hide. This is a feature: it means every design decision is defensible in an interview because it was a deliberate choice, not a framework default.

---

## Known Limitations

These are not oversights — they are deliberate scope decisions. Each represents a real distributed systems problem that production implementations solve.

**No linearizable reads.** `GET` is served from local leader state. A network-partitioned leader that has not yet been deposed will serve stale reads. Fix: route reads through a no-op log entry (Section 6.4 of the Raft paper) or use lease-based reads.

**No log compaction.** The log grows without bound. A node that has been down for a long time must replay the entire log from index 1. Fix: implement snapshotting (Section 7) — periodically serialize the KV state to disk, truncate the log, and ship snapshots to lagging followers via `InstallSnapshot` RPC.

**No membership changes.** The cluster is fixed at 3 nodes. Adding or removing nodes requires restarting the cluster. Fix: implement joint consensus (Section 6) for safe configuration changes.

**Single-datacenter only.** Election timeouts assume sub-millisecond one-way latency. Wide-area deployments require tuning timeouts to match actual RTT, or adopting a leader-leasing scheme to reduce cross-datacenter round trips on reads.

**No client session semantics.** If a client retries a write after a leader crash, the write may be applied twice. Fix: assign each client request a unique ID; the leader deduplicates on the session table before applying.

---

## References

- Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm.* USENIX ATC.
- Ongaro, D. (2014). *Consensus: Bridging Theory and Practice.* PhD thesis, Stanford University. (The extended Raft dissertation — more complete than the paper.)
- Howard, H. (2020). *Flexible Paxos: Quorum intersection revisited.* (Context for understanding what Raft's majority requirement actually buys you.)

---

## License

MIT
