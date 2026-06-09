#define _GNU_SOURCE
#include "kv.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Fixed-capacity open-addressing table with linear probing. The table is
 * purely derived state: it is rebuilt by replaying the log on startup, so it
 * never needs to be persisted. Deletes use tombstones to keep probe chains
 * intact. */

#define KV_CAP 16384 /* power of two; load factor stays well under 0.5 */

enum slot_state { SLOT_EMPTY = 0, SLOT_OCCUPIED, SLOT_TOMBSTONE };

typedef struct {
    int  state;
    char key[KV_KEY_SIZE];
    char val[KV_VAL_SIZE];
} slot_t;

struct kv_store {
    slot_t *slots;
    size_t  count;
};

static size_t hash_key(const char *key) {
    /* FNV-1a */
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        h ^= *p;
        h *= 1099511628211ULL;
    }
    return (size_t)(h & (KV_CAP - 1));
}

kv_store *kv_create(void) {
    kv_store *kv = malloc(sizeof *kv);
    kv->slots = calloc(KV_CAP, sizeof(slot_t));
    kv->count = 0;
    return kv;
}

void kv_destroy(kv_store *kv) {
    if (!kv) return;
    free(kv->slots);
    free(kv);
}

void kv_set(kv_store *kv, const char *key, const char *val) {
    size_t i = hash_key(key);
    size_t first_tomb = (size_t)-1;
    for (size_t probe = 0; probe < KV_CAP; probe++) {
        slot_t *s = &kv->slots[i];
        if (s->state == SLOT_OCCUPIED && strncmp(s->key, key, KV_KEY_SIZE) == 0) {
            strncpy(s->val, val, KV_VAL_SIZE - 1);
            s->val[KV_VAL_SIZE - 1] = '\0';
            return;
        }
        if (s->state == SLOT_TOMBSTONE && first_tomb == (size_t)-1) {
            first_tomb = i;
        }
        if (s->state == SLOT_EMPTY) {
            size_t dst = (first_tomb != (size_t)-1) ? first_tomb : i;
            slot_t *t = &kv->slots[dst];
            t->state = SLOT_OCCUPIED;
            strncpy(t->key, key, KV_KEY_SIZE - 1);
            t->key[KV_KEY_SIZE - 1] = '\0';
            strncpy(t->val, val, KV_VAL_SIZE - 1);
            t->val[KV_VAL_SIZE - 1] = '\0';
            kv->count++;
            return;
        }
        i = (i + 1) & (KV_CAP - 1);
    }
    /* Table full: silently drop. KV_CAP is sized to never reach here. */
}

void kv_del(kv_store *kv, const char *key) {
    size_t i = hash_key(key);
    for (size_t probe = 0; probe < KV_CAP; probe++) {
        slot_t *s = &kv->slots[i];
        if (s->state == SLOT_EMPTY) return;
        if (s->state == SLOT_OCCUPIED && strncmp(s->key, key, KV_KEY_SIZE) == 0) {
            s->state = SLOT_TOMBSTONE;
            kv->count--;
            return;
        }
        i = (i + 1) & (KV_CAP - 1);
    }
}

int kv_get(kv_store *kv, const char *key, char *out, size_t outsz) {
    size_t i = hash_key(key);
    for (size_t probe = 0; probe < KV_CAP; probe++) {
        slot_t *s = &kv->slots[i];
        if (s->state == SLOT_EMPTY) return 0;
        if (s->state == SLOT_OCCUPIED && strncmp(s->key, key, KV_KEY_SIZE) == 0) {
            strncpy(out, s->val, outsz - 1);
            out[outsz - 1] = '\0';
            return 1;
        }
        i = (i + 1) & (KV_CAP - 1);
    }
    return 0;
}
