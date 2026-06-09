#define _GNU_SOURCE
#include "timer.h"

#include <stdlib.h>
#include <time.h>

long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int rand_between(unsigned *state, int lo, int hi) {
    if (hi <= lo) return lo;
    return lo + (int)(rand_r(state) % (unsigned)(hi - lo + 1));
}
