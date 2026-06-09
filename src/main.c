#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "raft.h"

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --id <n> --port <p> --peers <p1,p2,...> [--datadir <dir>]\n",
            prog);
    exit(2);
}

/* Parse a comma-separated list of ports into out[]; returns the count. */
static int parse_peers(char *s, int *out, int max) {
    int n = 0;
    char *save = NULL;
    for (char *tok = strtok_r(s, ",", &save); tok && n < max;
         tok = strtok_r(NULL, ",", &save)) {
        out[n++] = atoi(tok);
    }
    return n;
}

int main(int argc, char **argv) {
    /* A peer dying mid-write would otherwise kill us with SIGPIPE. */
    signal(SIGPIPE, SIG_IGN);

    int id = -1, port = -1, num_peers = 0;
    int peers[MAX_PEERS];
    const char *datadir = ".";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            id = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--peers") == 0 && i + 1 < argc) {
            num_peers = parse_peers(argv[++i], peers, MAX_PEERS);
        } else if (strcmp(argv[i], "--datadir") == 0 && i + 1 < argc) {
            datadir = argv[++i];
        } else {
            usage(argv[0]);
        }
    }
    if (id < 0 || port < 0) usage(argv[0]);

    raft_t raft;
    raft_init(&raft, id, port, peers, num_peers, datadir);
    raft_start_background(&raft);
    raft_run_listener(&raft); /* blocks forever */
    return 0;
}
