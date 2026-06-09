#define _GNU_SOURCE
#include "net.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ------------------------------ buffer ---------------------------- */

void buf_init(buf_t *b) {
    b->cap = 64;
    b->len = 0;
    b->data = malloc(b->cap);
}

void buf_free(buf_t *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

static void buf_reserve(buf_t *b, size_t extra) {
    if (b->len + extra <= b->cap) return;
    while (b->len + extra > b->cap) b->cap *= 2;
    b->data = realloc(b->data, b->cap);
}

void buf_put(buf_t *b, const void *p, size_t n) {
    buf_reserve(b, n);
    memcpy(b->data + b->len, p, n);
    b->len += n;
}

void buf_put_u8(buf_t *b, uint8_t v) { buf_put(b, &v, 1); }

void buf_put_u16(buf_t *b, uint16_t v) {
    uint8_t t[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    buf_put(b, t, 2);
}

void buf_put_u32(buf_t *b, uint32_t v) {
    uint8_t t[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16),
                    (uint8_t)(v >> 8), (uint8_t)v};
    buf_put(b, t, 4);
}

void buf_put_u64(buf_t *b, uint64_t v) {
    uint8_t t[8];
    for (int i = 0; i < 8; i++) t[i] = (uint8_t)(v >> (56 - 8 * i));
    buf_put(b, t, 8);
}

void buf_put_str(buf_t *b, const char *s, uint16_t n) {
    buf_put_u16(b, n);
    buf_put(b, s, n);
}

/* ------------------------------ reader ---------------------------- */

void rdr_init(rdr_t *r, const uint8_t *data, size_t len) {
    r->data = data;
    r->len = len;
    r->pos = 0;
}

static int rdr_take(rdr_t *r, void *out, size_t n) {
    if (r->pos + n > r->len) return -1;
    memcpy(out, r->data + r->pos, n);
    r->pos += n;
    return 0;
}

int rdr_u8(rdr_t *r, uint8_t *out) { return rdr_take(r, out, 1); }

int rdr_u16(rdr_t *r, uint16_t *out) {
    uint8_t t[2];
    if (rdr_take(r, t, 2)) return -1;
    *out = (uint16_t)t[0] << 8 | t[1];
    return 0;
}

int rdr_u32(rdr_t *r, uint32_t *out) {
    uint8_t t[4];
    if (rdr_take(r, t, 4)) return -1;
    *out = (uint32_t)t[0] << 24 | (uint32_t)t[1] << 16 |
           (uint32_t)t[2] << 8 | t[3];
    return 0;
}

int rdr_u64(rdr_t *r, uint64_t *out) {
    uint8_t t[8];
    if (rdr_take(r, t, 8)) return -1;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = v << 8 | t[i];
    *out = v;
    return 0;
}

int rdr_str_raw(rdr_t *r, char *out, size_t n) {
    return rdr_take(r, out, n);
}

int rdr_str(rdr_t *r, char *out, uint16_t max, uint16_t *outlen) {
    uint16_t n;
    if (rdr_u16(r, &n)) return -1;
    if (n >= max) return -1;
    if (rdr_take(r, out, n)) return -1;
    out[n] = '\0';
    if (outlen) *outlen = n;
    return 0;
}

/* ------------------------------ sockets --------------------------- */

ssize_t recv_all(int fd, void *buf, size_t n) {
    size_t received = 0;
    while (received < n) {
        ssize_t r = read(fd, (char *)buf + received, n - received);
        if (r < 0) {
            if (errno == EINTR) continue;
            return r;
        }
        if (r == 0) return 0; /* peer closed */
        received += (size_t)r;
    }
    return (ssize_t)received;
}

ssize_t send_all(int fd, const void *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = write(fd, (const char *)buf + sent, n - sent);
        if (w < 0) {
            if (errno == EINTR) continue;
            return w;
        }
        sent += (size_t)w;
    }
    return (ssize_t)sent;
}

int send_msg(int fd, uint8_t type, const uint8_t *payload, uint32_t len) {
    if (len > 0xFFFFFF) return -1;
    uint8_t hdr[4];
    hdr[0] = type;
    hdr[1] = (uint8_t)(len >> 16);
    hdr[2] = (uint8_t)(len >> 8);
    hdr[3] = (uint8_t)len;
    if (send_all(fd, hdr, 4) != 4) return -1;
    if (len && send_all(fd, payload, len) != (ssize_t)len) return -1;
    return 0;
}

int recv_msg(int fd, uint8_t *type, uint8_t **payload, uint32_t *len) {
    uint8_t hdr[4];
    ssize_t r = recv_all(fd, hdr, 4);
    if (r != 4) return -1;
    *type = hdr[0];
    uint32_t n = (uint32_t)hdr[1] << 16 | (uint32_t)hdr[2] << 8 | hdr[3];
    *len = n;
    if (n == 0) {
        *payload = NULL;
        return 0;
    }
    *payload = malloc(n);
    if (!*payload) return -1;
    if (recv_all(fd, *payload, n) != (ssize_t)n) {
        free(*payload);
        *payload = NULL;
        return -1;
    }
    return 0;
}

int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 64) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int tcp_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        close(fd);
        return -1;
    }

    /* Bound blocking reads/writes so a dead peer never wedges a thread. */
    if (timeout_ms > 0) {
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}
