#define _GNU_SOURCE
#include "raft.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"

/* On-disk record layout for each log entry (big-endian):
 *   index:8  term:4  cmd_len:2  cmd:cmd_len
 * meta.bin holds: current_term:4  voted_for(int32):4
 *
 * Both files are opened O_DSYNC: a write() does not return until the data is
 * on stable storage, closing the window a separate fsync() would leave open.
 */

static size_t record_size(const log_entry_t *e) {
    return 8 + 4 + 2 + e->cmd_len;
}

static void log_grow(raft_t *r, uint64_t need) {
    if (need < r->log_cap) return;
    uint64_t cap = r->log_cap ? r->log_cap : 16;
    while (need >= cap) cap *= 2;
    r->log = realloc(r->log, cap * sizeof(log_entry_t));
    r->log_cap = cap;
}

static void write_record(raft_t *r, const log_entry_t *e) {
    buf_t b;
    buf_init(&b);
    buf_put_u64(&b, e->index);
    buf_put_u32(&b, e->term);
    buf_put_u16(&b, e->cmd_len);
    buf_put(&b, e->cmd, e->cmd_len);
    if (send_all(r->log_fd, b.data, b.len) != (ssize_t)b.len) {
        perror("log write");
        exit(1);
    }
    r->log_file_size += (off_t)b.len;
    buf_free(&b);
}

/* Read the whole log file back into memory at startup. */
static void log_load(raft_t *r) {
    if (lseek(r->log_fd, 0, SEEK_SET) < 0) return;
    r->log_count = 0;
    for (;;) {
        uint8_t hdr[14];
        ssize_t got = recv_all(r->log_fd, hdr, 14);
        if (got == 0) break;     /* clean EOF */
        if (got != 14) break;    /* torn tail: stop, treat as not present */

        rdr_t rd;
        rdr_init(&rd, hdr, 14);
        uint64_t index;
        uint32_t term;
        uint16_t cmd_len;
        rdr_u64(&rd, &index);
        rdr_u32(&rd, &term);
        rdr_u16(&rd, &cmd_len);
        if (cmd_len >= CMD_SIZE) break;

        log_entry_t e;
        memset(&e, 0, sizeof e);
        e.index = index;
        e.term = term;
        e.cmd_len = cmd_len;
        if (cmd_len && recv_all(r->log_fd, e.cmd, cmd_len) != cmd_len) break;
        e.cmd[cmd_len] = '\0';

        log_grow(r, index);
        r->log[index] = e;
        r->log_count = index;
    }
    r->log_file_size = lseek(r->log_fd, 0, SEEK_END);
}

void meta_persist(raft_t *r) {
    uint8_t buf[8];
    buf[0] = (uint8_t)(r->current_term >> 24);
    buf[1] = (uint8_t)(r->current_term >> 16);
    buf[2] = (uint8_t)(r->current_term >> 8);
    buf[3] = (uint8_t)r->current_term;
    int32_t vf = (int32_t)r->voted_for;
    buf[4] = (uint8_t)(vf >> 24);
    buf[5] = (uint8_t)(vf >> 16);
    buf[6] = (uint8_t)(vf >> 8);
    buf[7] = (uint8_t)vf;
    if (pwrite(r->meta_fd, buf, 8, 0) != 8) {
        perror("meta write");
        exit(1);
    }
}

static void meta_load(raft_t *r) {
    uint8_t buf[8];
    ssize_t got = pread(r->meta_fd, buf, 8, 0);
    if (got != 8) {
        r->current_term = 0;
        r->voted_for = -1;
        return;
    }
    r->current_term = (uint32_t)buf[0] << 24 | (uint32_t)buf[1] << 16 |
                      (uint32_t)buf[2] << 8 | buf[3];
    int32_t vf = (int32_t)((uint32_t)buf[4] << 24 | (uint32_t)buf[5] << 16 |
                           (uint32_t)buf[6] << 8 | buf[7]);
    r->voted_for = vf;
}

void log_open(raft_t *r, const char *datadir) {
    char path[512];

    snprintf(path, sizeof path, "%s/log_%d.bin", datadir, r->id);
    r->log_fd = open(path, O_RDWR | O_CREAT | O_APPEND | O_DSYNC, 0644);
    if (r->log_fd < 0) {
        perror("open log.bin");
        exit(1);
    }

    snprintf(path, sizeof path, "%s/meta_%d.bin", datadir, r->id);
    r->meta_fd = open(path, O_RDWR | O_CREAT | O_DSYNC, 0644);
    if (r->meta_fd < 0) {
        perror("open meta.bin");
        exit(1);
    }

    /* sentinel entry at index 0 */
    log_grow(r, 0);
    memset(&r->log[0], 0, sizeof(log_entry_t));
    r->log_count = 0;

    meta_load(r);
    log_load(r);
}

void log_append_entry(raft_t *r, const log_entry_t *e) {
    log_grow(r, e->index);
    r->log[e->index] = *e;
    r->log_count = e->index;
    write_record(r, e); /* durable before we return */
}

void log_truncate(raft_t *r, uint64_t keep_index) {
    if (keep_index >= r->log_count) return;
    off_t offset = 0;
    for (uint64_t i = 1; i <= keep_index; i++) offset += (off_t)record_size(&r->log[i]);
    if (ftruncate(r->log_fd, offset) < 0) {
        perror("ftruncate log");
        exit(1);
    }
    r->log_file_size = offset;
    r->log_count = keep_index;
}

uint32_t log_term_at(raft_t *r, uint64_t index) {
    if (index == 0 || index > r->log_count) return 0;
    return r->log[index].term;
}
