#ifndef RAFT_H
#define RAFT_H

#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>

#include "kv.h"

#define MAX_PEERS 16
#define CMD_SIZE  256 /* "SET <64> <64>" fits comfortably */

typedef enum { FOLLOWER, CANDIDATE, LEADER } raft_state_t;

typedef struct {
    uint64_t index;
    uint32_t term;
    uint16_t cmd_len;
    char     cmd[CMD_SIZE];
} log_entry_t;

typedef struct {
    /* ---- identity / cluster config (immutable after init) ---- */
    int id;
    int port;
    int peer_ports[MAX_PEERS];
    int num_peers;

    /* ---- persistent state (durable before any RPC reply) ---- */
    uint32_t current_term;
    int      voted_for; /* -1 = none */
    log_entry_t *log;   /* 1-indexed; log[0] is a sentinel */
    uint64_t log_count; /* index of last entry (0 = empty) */
    uint64_t log_cap;

    /* ---- volatile state ---- */
    raft_state_t state;
    uint64_t     commit_index;
    uint64_t     last_applied;
    int          leader_port; /* last known leader's client port; 0 if none */

    /* ---- leader-only volatile state (per peer) ---- */
    uint64_t next_index[MAX_PEERS];
    uint64_t match_index[MAX_PEERS];

    /* ---- election timing ---- */
    long     last_reset_ms;
    long     election_timeout_ms;
    unsigned rand_state;

    /* ---- persistence ---- */
    int   log_fd;
    int   meta_fd;
    off_t log_file_size;

    /* ---- synchronization ---- */
    pthread_mutex_t mu;
    pthread_cond_t  repl_cond;  /* kick the leader to replicate now */
    pthread_cond_t  apply_cond; /* last_applied advanced */
    int             running;

    kv_store *kv;
} raft_t;

/* Lifecycle. */
void raft_init(raft_t *r, int id, int port, int *peer_ports, int num_peers,
               const char *datadir);
void raft_start_background(raft_t *r); /* election, leader, apply threads */
void raft_run_listener(raft_t *r);     /* blocks: accept + dispatch loop */

/* ---- log.c (persistent log) ---- */
void log_open(raft_t *r, const char *datadir);
void log_append_entry(raft_t *r, const log_entry_t *e); /* in-mem + durable */
void log_truncate(raft_t *r, uint64_t keep_index);      /* drop entries > keep */
uint32_t log_term_at(raft_t *r, uint64_t index);        /* 0 for index 0 */
void meta_persist(raft_t *r);                           /* term + voted_for */

#endif /* RAFT_H */
