#ifndef NET_H
#define NET_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

/* Wire message types (1-byte frame header tag). */
enum {
    MSG_REQUEST_VOTE       = 0x01, /* Candidate -> Peers   */
    MSG_REQUEST_VOTE_RESP  = 0x02, /* Peer -> Candidate     */
    MSG_APPEND_ENTRIES     = 0x03, /* Leader -> Followers   */
    MSG_APPEND_ENTRIES_RESP= 0x04, /* Follower -> Leader    */
    MSG_CLIENT_REQUEST     = 0x05, /* Client -> Any node    */
    MSG_CLIENT_RESPONSE    = 0x06, /* Node -> Client        */
    MSG_REDIRECT           = 0x07, /* Follower -> Client    */
};

/* ------------------------------------------------------------------ */
/* Growable write buffer for encoding payloads (big-endian integers).  */
/* ------------------------------------------------------------------ */
typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} buf_t;

void buf_init(buf_t *b);
void buf_free(buf_t *b);
void buf_put(buf_t *b, const void *p, size_t n);
void buf_put_u8(buf_t *b, uint8_t v);
void buf_put_u16(buf_t *b, uint16_t v);
void buf_put_u32(buf_t *b, uint32_t v);
void buf_put_u64(buf_t *b, uint64_t v);
/* length-prefixed string: 2-byte length, then bytes (no terminator). */
void buf_put_str(buf_t *b, const char *s, uint16_t n);

/* ------------------------------------------------------------------ */
/* Cursor-based reader. Every accessor returns 0 on success, -1 if the */
/* read would run past the end of the buffer.                          */
/* ------------------------------------------------------------------ */
typedef struct {
    const uint8_t *data;
    size_t         len;
    size_t         pos;
} rdr_t;

void rdr_init(rdr_t *r, const uint8_t *data, size_t len);
int  rdr_u8(rdr_t *r, uint8_t *out);
int  rdr_u16(rdr_t *r, uint16_t *out);
int  rdr_u32(rdr_t *r, uint32_t *out);
int  rdr_u64(rdr_t *r, uint64_t *out);
/* reads a 2-byte length, then up to `max` bytes into out (NUL-terminated). */
int  rdr_str(rdr_t *r, char *out, uint16_t max, uint16_t *outlen);
/* reads exactly n raw bytes (no length prefix) into out. */
int  rdr_str_raw(rdr_t *r, char *out, size_t n);

/* ------------------------------------------------------------------ */
/* Socket helpers. Every network read goes through recv_all.           */
/* ------------------------------------------------------------------ */
ssize_t recv_all(int fd, void *buf, size_t n);
ssize_t send_all(int fd, const void *buf, size_t n);

/* Framed message: [1 byte type][3 byte BE length][payload]. */
int send_msg(int fd, uint8_t type, const uint8_t *payload, uint32_t len);
/* On success *payload is malloc'd (caller frees) and *len set. Returns 0,
 * -1 on error/EOF. */
int recv_msg(int fd, uint8_t *type, uint8_t **payload, uint32_t *len);

int tcp_listen(int port);
int tcp_connect(const char *host, int port, int timeout_ms);

#endif /* NET_H */
