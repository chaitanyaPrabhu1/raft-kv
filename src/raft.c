#define _GNU_SOURCE
#include "raft.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "net.h"
#include "timer.h"

#define HEARTBEAT_MS        100
#define ELECTION_MIN_MS     150
#define ELECTION_MAX_MS     300
#define RPC_TIMEOUT_MS      120
#define CLIENT_WAIT_MS      3000

/* ===================================================================== */
/*  Small helpers                                                         */
/* ===================================================================== */

static int majority(raft_t *r) {
    int total = r->num_peers + 1;
    return total / 2 + 1;
}

static uint64_t last_log_index(raft_t *r) { return r->log_count; }
static uint32_t last_log_term(raft_t *r) { return log_term_at(r, r->log_count); }

static void reset_election_timer(raft_t *r) {
    r->last_reset_ms = now_ms();
    r->election_timeout_ms = rand_between(&r->rand_state, ELECTION_MIN_MS, ELECTION_MAX_MS);
}

/* Caller must hold r->mu. Step down / adopt a newer term. */
static void become_follower(raft_t *r, uint32_t term) {
    if (term > r->current_term) {
        r->current_term = term;
        r->voted_for = -1;
        meta_persist(r);
    }
    r->state = FOLLOWER;
}

static int cond_wait_ms(pthread_cond_t *c, pthread_mutex_t *m, int ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }
    return pthread_cond_timedwait(c, m, &ts);
}

/* One-shot RPC: connect, send, read a single response, close. */
static int rpc_call(int port, uint8_t type, const uint8_t *payload, uint32_t len,
                    uint8_t *rtype, uint8_t **rpayload, uint32_t *rlen) {
    int fd = tcp_connect("127.0.0.1", port, RPC_TIMEOUT_MS);
    if (fd < 0) return -1;
    int rc = -1;
    if (send_msg(fd, type, payload, len) == 0 &&
        recv_msg(fd, rtype, rpayload, rlen) == 0) {
        rc = 0;
    }
    close(fd);
    return rc;
}

/* ===================================================================== */
/*  State machine: apply committed commands to the KV store               */
/* ===================================================================== */

static void apply_command(raft_t *r, const char *cmd) {
    char buf[CMD_SIZE];
    strncpy(buf, cmd, sizeof buf - 1);
    buf[sizeof buf - 1] = '\0';

    char *save = NULL;
    char *op = strtok_r(buf, " ", &save);
    if (!op) return;
    if (strcmp(op, "SET") == 0) {
        char *key = strtok_r(NULL, " ", &save);
        char *val = strtok_r(NULL, "", &save); /* rest of line */
        if (key && val) kv_set(r->kv, key, val);
    } else if (strcmp(op, "DEL") == 0) {
        char *key = strtok_r(NULL, " ", &save);
        if (key) kv_del(r->kv, key);
    }
}

/* Caller must hold r->mu. Advance commit_index on the leader: the highest N
 * replicated to a majority whose entry is from the current term. */
static void leader_advance_commit(raft_t *r) {
    for (uint64_t n = r->log_count; n > r->commit_index; n--) {
        if (log_term_at(r, n) != r->current_term) continue;
        int count = 1; /* self */
        for (int i = 0; i < r->num_peers; i++)
            if (r->match_index[i] >= n) count++;
        if (count >= majority(r)) {
            r->commit_index = n;
            pthread_cond_broadcast(&r->apply_cond);
            break;
        }
    }
}

/* ===================================================================== */
/*  RPC handlers (each returns an encoded response payload in *out)        */
/* ===================================================================== */

static void handle_request_vote(raft_t *r, const uint8_t *p, uint32_t len, buf_t *out) {
    rdr_t rd;
    rdr_init(&rd, p, len);
    uint32_t term, cand_id, cand_last_term;
    uint64_t cand_last_index;
    if (rdr_u32(&rd, &term) || rdr_u32(&rd, &cand_id) ||
        rdr_u64(&rd, &cand_last_index) || rdr_u32(&rd, &cand_last_term)) {
        return;
    }

    pthread_mutex_lock(&r->mu);
    uint8_t grant = 0;
    if (term > r->current_term) become_follower(r, term);

    if (term >= r->current_term) {
        uint32_t my_term = last_log_term(r);
        uint64_t my_index = last_log_index(r);
        int up_to_date = (cand_last_term > my_term) ||
                         (cand_last_term == my_term && cand_last_index >= my_index);
        if ((r->voted_for == -1 || r->voted_for == (int)cand_id) && up_to_date) {
            r->voted_for = (int)cand_id;
            meta_persist(r);
            grant = 1;
            reset_election_timer(r); /* we acknowledged a viable candidate */
        }
    }
    uint32_t resp_term = r->current_term;
    pthread_mutex_unlock(&r->mu);

    buf_put_u32(out, resp_term);
    buf_put_u8(out, grant);
}

static void handle_append_entries(raft_t *r, const uint8_t *p, uint32_t len, buf_t *out) {
    rdr_t rd;
    rdr_init(&rd, p, len);
    uint32_t term, leader_id, leader_port, prev_term;
    uint64_t prev_index, leader_commit;
    uint32_t count;
    if (rdr_u32(&rd, &term) || rdr_u32(&rd, &leader_id) || rdr_u32(&rd, &leader_port) ||
        rdr_u64(&rd, &prev_index) || rdr_u32(&rd, &prev_term) ||
        rdr_u64(&rd, &leader_commit) || rdr_u32(&rd, &count)) {
        return;
    }

    pthread_mutex_lock(&r->mu);
    uint8_t success = 0;
    uint64_t match = 0;

    if (term < r->current_term) {
        /* stale leader; reject without touching our state */
        uint32_t rt = r->current_term;
        pthread_mutex_unlock(&r->mu);
        buf_put_u32(out, rt);
        buf_put_u8(out, 0);
        buf_put_u64(out, 0);
        return;
    }

    if (term > r->current_term) become_follower(r, term);
    r->state = FOLLOWER;
    r->leader_port = (int)leader_port;
    reset_election_timer(r);

    int consistent = (prev_index <= r->log_count) &&
                     (prev_index == 0 || log_term_at(r, prev_index) == prev_term);

    if (!consistent) {
        /* If our entry at prev_index conflicts, drop it and everything after
         * so the leader can backfill from an agreed prefix. */
        if (prev_index <= r->log_count && prev_index > 0) log_truncate(r, prev_index - 1);
        success = 0;
    } else {
        for (uint32_t k = 0; k < count; k++) {
            log_entry_t e;
            memset(&e, 0, sizeof e);
            if (rdr_u64(&rd, &e.index) || rdr_u32(&rd, &e.term) || rdr_u16(&rd, &e.cmd_len))
                break;
            if (e.cmd_len >= CMD_SIZE) break;
            if (e.cmd_len && rdr_str_raw(&rd, e.cmd, e.cmd_len)) break;
            e.cmd[e.cmd_len] = '\0';

            if (e.index <= r->log_count) {
                if (log_term_at(r, e.index) != e.term) {
                    log_truncate(r, e.index - 1);
                    log_append_entry(r, &e);
                }
                /* else: identical entry already present, skip */
            } else {
                log_append_entry(r, &e);
            }
        }
        success = 1;
        match = prev_index + count;

        if (leader_commit > r->commit_index) {
            uint64_t newc = leader_commit < r->log_count ? leader_commit : r->log_count;
            if (newc > r->commit_index) {
                r->commit_index = newc;
                pthread_cond_broadcast(&r->apply_cond);
            }
        }
    }

    uint32_t resp_term = r->current_term;
    pthread_mutex_unlock(&r->mu);

    buf_put_u32(out, resp_term);
    buf_put_u8(out, success);
    buf_put_u64(out, match);
}

/* ===================================================================== */
/*  Client request handling                                               */
/* ===================================================================== */

static void send_redirect(int fd, int leader_port) {
    buf_t b;
    buf_init(&b);
    buf_put_u32(&b, (uint32_t)leader_port);
    send_msg(fd, MSG_REDIRECT, b.data, (uint32_t)b.len);
    buf_free(&b);
}

static void send_client_response(int fd, uint8_t status, const char *msg) {
    buf_t b;
    buf_init(&b);
    buf_put_u8(&b, status);
    uint16_t n = (uint16_t)strlen(msg);
    buf_put_str(&b, msg, n);
    send_msg(fd, MSG_CLIENT_RESPONSE, b.data, (uint32_t)b.len);
    buf_free(&b);
}

static void handle_client_request(raft_t *r, int fd, const uint8_t *p, uint32_t len) {
    rdr_t rd;
    rdr_init(&rd, p, len);
    char cmd[CMD_SIZE];
    uint16_t cmd_len;
    if (rdr_str(&rd, cmd, CMD_SIZE, &cmd_len)) {
        send_client_response(fd, 1, "ERR malformed request");
        return;
    }

    /* GET is served from local leader state (not linearizable). */
    int is_read = (strncmp(cmd, "GET ", 4) == 0);

    pthread_mutex_lock(&r->mu);
    if (r->state != LEADER) {
        int lp = r->leader_port;
        pthread_mutex_unlock(&r->mu);
        if (lp > 0) send_redirect(fd, lp);
        else send_client_response(fd, 1, "ERR no leader");
        return;
    }

    if (is_read) {
        char key[KV_KEY_SIZE], val[KV_VAL_SIZE];
        char tmp[CMD_SIZE];
        strncpy(tmp, cmd, sizeof tmp - 1);
        tmp[sizeof tmp - 1] = '\0';
        char *save = NULL;
        strtok_r(tmp, " ", &save);              /* GET */
        char *k = strtok_r(NULL, " ", &save);
        int found = 0;
        if (k) {
            strncpy(key, k, sizeof key - 1);
            key[sizeof key - 1] = '\0';
            found = kv_get(r->kv, key, val, sizeof val);
        }
        pthread_mutex_unlock(&r->mu);
        send_client_response(fd, 0, found ? val : "(nil)");
        return;
    }

    /* SET / DEL: replicate through the log. */
    log_entry_t e;
    memset(&e, 0, sizeof e);
    e.index = r->log_count + 1;
    e.term = r->current_term;
    e.cmd_len = cmd_len;
    memcpy(e.cmd, cmd, cmd_len);
    log_append_entry(r, &e);

    uint64_t idx = e.index;
    uint32_t term = e.term;
    pthread_cond_broadcast(&r->repl_cond); /* kick replication now */

    long deadline = now_ms() + CLIENT_WAIT_MS;
    int committed = 0;
    while (r->running && r->state == LEADER && r->current_term == term) {
        if (r->last_applied >= idx) {
            committed = 1;
            break;
        }
        int remain = (int)(deadline - now_ms());
        if (remain <= 0) break;
        cond_wait_ms(&r->apply_cond, &r->mu, remain);
    }
    int still_leader = (r->state == LEADER && r->current_term == term);
    int lp = r->leader_port;
    pthread_mutex_unlock(&r->mu);

    if (committed) send_client_response(fd, 0, "OK");
    else if (!still_leader && lp > 0) send_redirect(fd, lp);
    else send_client_response(fd, 1, "ERR not committed");
}

/* ===================================================================== */
/*  Connection dispatch (one detached thread per accepted connection)     */
/* ===================================================================== */

typedef struct { raft_t *r; int fd; } conn_arg_t;

static void *conn_thread(void *arg) {
    conn_arg_t *ca = arg;
    raft_t *r = ca->r;
    int fd = ca->fd;
    free(ca);

    for (;;) {
        uint8_t type;
        uint8_t *payload = NULL;
        uint32_t len = 0;
        if (recv_msg(fd, &type, &payload, &len) != 0) break;

        if (type == MSG_REQUEST_VOTE) {
            buf_t out;
            buf_init(&out);
            handle_request_vote(r, payload, len, &out);
            send_msg(fd, MSG_REQUEST_VOTE_RESP, out.data, (uint32_t)out.len);
            buf_free(&out);
        } else if (type == MSG_APPEND_ENTRIES) {
            buf_t out;
            buf_init(&out);
            handle_append_entries(r, payload, len, &out);
            send_msg(fd, MSG_APPEND_ENTRIES_RESP, out.data, (uint32_t)out.len);
            buf_free(&out);
        } else if (type == MSG_CLIENT_REQUEST) {
            handle_client_request(r, fd, payload, len);
        } else {
            free(payload);
            break;
        }
        free(payload);
    }
    close(fd);
    return NULL;
}

/* ===================================================================== */
/*  Election                                                              */
/* ===================================================================== */

static void become_leader(raft_t *r) {
    r->state = LEADER;
    r->leader_port = r->port;
    for (int i = 0; i < r->num_peers; i++) {
        r->next_index[i] = r->log_count + 1;
        r->match_index[i] = 0;
    }

    /* Append a no-op entry in the current term. A leader may only advance
     * commit_index over an entry from its own term (Raft §5.4.2, Figure 8).
     * Committing this no-op therefore commits every preceding entry too,
     * which is what re-applies entries recovered from disk after a restart.
     * apply_command ignores any command that isn't SET/DEL, so "NOOP" has
     * no effect on the KV store. */
    log_entry_t noop;
    memset(&noop, 0, sizeof noop);
    noop.index = r->log_count + 1;
    noop.term = r->current_term;
    noop.cmd_len = 4;
    memcpy(noop.cmd, "NOOP", 4);
    log_append_entry(r, &noop);

    fprintf(stderr, "[node %d] became LEADER for term %u\n", r->id, r->current_term);
    pthread_cond_broadcast(&r->repl_cond);
}

static void run_election(raft_t *r) {
    /* Caller holds the lock. Prepare candidacy, then release for I/O. */
    r->current_term++;
    r->state = CANDIDATE;
    r->voted_for = r->id;
    meta_persist(r);
    reset_election_timer(r);

    uint32_t term = r->current_term;
    uint64_t li = last_log_index(r);
    uint32_t lt = last_log_term(r);
    int peers[MAX_PEERS], np = r->num_peers;
    memcpy(peers, r->peer_ports, sizeof(int) * np);
    int id = r->id;
    pthread_mutex_unlock(&r->mu);

    buf_t req;
    buf_init(&req);
    buf_put_u32(&req, term);
    buf_put_u32(&req, (uint32_t)id);
    buf_put_u64(&req, li);
    buf_put_u32(&req, lt);

    int votes = 1; /* self */
    uint32_t max_term = term;
    for (int i = 0; i < np; i++) {
        uint8_t rtype, *rp = NULL;
        uint32_t rl = 0;
        if (rpc_call(peers[i], MSG_REQUEST_VOTE, req.data, (uint32_t)req.len,
                     &rtype, &rp, &rl) == 0 && rtype == MSG_REQUEST_VOTE_RESP) {
            rdr_t rd;
            rdr_init(&rd, rp, rl);
            uint32_t rterm = 0;
            uint8_t grant = 0;
            rdr_u32(&rd, &rterm);
            rdr_u8(&rd, &grant);
            if (rterm > max_term) max_term = rterm;
            if (grant && rterm == term) votes++;
        }
        free(rp);
    }
    buf_free(&req);

    pthread_mutex_lock(&r->mu);
    if (max_term > r->current_term) {
        become_follower(r, max_term);
    } else if (r->state == CANDIDATE && r->current_term == term) {
        if (votes >= majority(r)) become_leader(r);
    }
}

static void *election_thread(void *arg) {
    raft_t *r = arg;
    pthread_mutex_lock(&r->mu);
    while (r->running) {
        if (r->state != LEADER &&
            now_ms() - r->last_reset_ms >= r->election_timeout_ms) {
            run_election(r); /* releases & re-acquires the lock */
        }
        cond_wait_ms(&r->repl_cond, &r->mu, 15);
    }
    pthread_mutex_unlock(&r->mu);
    return NULL;
}

/* ===================================================================== */
/*  Leader replication                                                    */
/* ===================================================================== */

static void *leader_thread(void *arg) {
    raft_t *r = arg;
    pthread_mutex_lock(&r->mu);
    while (r->running) {
        if (r->state != LEADER) {
            cond_wait_ms(&r->repl_cond, &r->mu, 50);
            continue;
        }
        uint32_t term = r->current_term;

        for (int i = 0; i < r->num_peers && r->state == LEADER && r->current_term == term; i++) {
            uint64_t ni = r->next_index[i];
            if (ni < 1) ni = 1;
            uint64_t prev = ni - 1;
            uint32_t prevterm = log_term_at(r, prev);
            uint64_t last_sent = r->log_count;
            uint64_t commit = r->commit_index;
            int peer_port = r->peer_ports[i];

            buf_t req;
            buf_init(&req);
            buf_put_u32(&req, term);
            buf_put_u32(&req, (uint32_t)r->id);
            buf_put_u32(&req, (uint32_t)r->port);
            buf_put_u64(&req, prev);
            buf_put_u32(&req, prevterm);
            buf_put_u64(&req, commit);
            buf_put_u32(&req, (uint32_t)(last_sent - prev)); /* entry count */
            for (uint64_t j = ni; j <= last_sent; j++) {
                log_entry_t *e = &r->log[j];
                buf_put_u64(&req, e->index);
                buf_put_u32(&req, e->term);
                buf_put_u16(&req, e->cmd_len);
                buf_put(&req, e->cmd, e->cmd_len);
            }

            pthread_mutex_unlock(&r->mu);
            uint8_t rtype, *rp = NULL;
            uint32_t rl = 0;
            int ok = rpc_call(peer_port, MSG_APPEND_ENTRIES, req.data, (uint32_t)req.len,
                              &rtype, &rp, &rl);
            buf_free(&req);
            pthread_mutex_lock(&r->mu);

            if (ok == 0 && rtype == MSG_APPEND_ENTRIES_RESP) {
                rdr_t rd;
                rdr_init(&rd, rp, rl);
                uint32_t rterm = 0;
                uint8_t success = 0;
                uint64_t mi = 0;
                rdr_u32(&rd, &rterm);
                rdr_u8(&rd, &success);
                rdr_u64(&rd, &mi);

                if (rterm > r->current_term) {
                    become_follower(r, rterm);
                } else if (r->state == LEADER && r->current_term == term) {
                    if (success) {
                        r->match_index[i] = last_sent;
                        r->next_index[i] = last_sent + 1;
                    } else if (r->next_index[i] > 1) {
                        r->next_index[i]--;
                    }
                }
            }
            free(rp);
        }

        leader_advance_commit(r);
        cond_wait_ms(&r->repl_cond, &r->mu, HEARTBEAT_MS);
    }
    pthread_mutex_unlock(&r->mu);
    return NULL;
}

/* ===================================================================== */
/*  Apply thread (commit_index -> KV store)                               */
/* ===================================================================== */

static void *apply_thread(void *arg) {
    raft_t *r = arg;
    pthread_mutex_lock(&r->mu);
    while (r->running) {
        while (r->running && r->last_applied >= r->commit_index)
            cond_wait_ms(&r->apply_cond, &r->mu, 100);
        while (r->last_applied < r->commit_index) {
            r->last_applied++;
            apply_command(r, r->log[r->last_applied].cmd);
        }
        pthread_cond_broadcast(&r->apply_cond); /* wake client write waiters */
    }
    pthread_mutex_unlock(&r->mu);
    return NULL;
}

/* ===================================================================== */
/*  Public lifecycle                                                      */
/* ===================================================================== */

void raft_init(raft_t *r, int id, int port, int *peer_ports, int num_peers,
               const char *datadir) {
    memset(r, 0, sizeof *r);
    r->id = id;
    r->port = port;
    r->num_peers = num_peers;
    for (int i = 0; i < num_peers; i++) r->peer_ports[i] = peer_ports[i];

    r->state = FOLLOWER;
    r->voted_for = -1;
    r->commit_index = 0;
    r->last_applied = 0;
    r->leader_port = 0;
    r->running = 1;
    r->rand_state = (unsigned)(now_ms() ^ ((long)id * 2654435761u));

    pthread_mutex_init(&r->mu, NULL);
    pthread_cond_init(&r->repl_cond, NULL);
    pthread_cond_init(&r->apply_cond, NULL);

    r->kv = kv_create();
    log_open(r, datadir); /* restores current_term, voted_for, log */
    reset_election_timer(r);

    fprintf(stderr, "[node %d] started on port %d, term %u, %lu log entries\n",
            id, port, r->current_term, (unsigned long)r->log_count);
}

void raft_start_background(raft_t *r) {
    pthread_t t;
    pthread_create(&t, NULL, election_thread, r);
    pthread_detach(t);
    pthread_create(&t, NULL, leader_thread, r);
    pthread_detach(t);
    pthread_create(&t, NULL, apply_thread, r);
    pthread_detach(t);
}

void raft_run_listener(raft_t *r) {
    int lfd = tcp_listen(r->port);
    if (lfd < 0) {
        fprintf(stderr, "[node %d] failed to listen on port %d\n", r->id, r->port);
        exit(1);
    }
    for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) continue;
        conn_arg_t *ca = malloc(sizeof *ca);
        ca->r = r;
        ca->fd = cfd;
        pthread_t t;
        if (pthread_create(&t, NULL, conn_thread, ca) != 0) {
            close(cfd);
            free(ca);
            continue;
        }
        pthread_detach(t);
    }
}
