#ifndef TIMER_H
#define TIMER_H

/* Milliseconds from a monotonic clock (immune to wall-clock jumps). */
long now_ms(void);

/* Uniform random integer in [lo, hi], using a caller-owned rand_r state. */
int rand_between(unsigned *state, int lo, int hi);

#endif /* TIMER_H */
