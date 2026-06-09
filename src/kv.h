#ifndef KV_H
#define KV_H

#include <stddef.h>

#define KV_KEY_SIZE 64
#define KV_VAL_SIZE 64

typedef struct kv_store kv_store;

kv_store *kv_create(void);
void      kv_destroy(kv_store *kv);

/* key/value are NUL-terminated C strings, each < 64 bytes. */
void kv_set(kv_store *kv, const char *key, const char *val);
void kv_del(kv_store *kv, const char *key);
/* Returns 1 and copies the value into out (size outsz) if present, else 0. */
int  kv_get(kv_store *kv, const char *key, char *out, size_t outsz);

#endif /* KV_H */
