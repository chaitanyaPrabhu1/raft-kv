#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"

/* Send one command to `port`, following redirects up to a few hops. Prints
 * the node's reply. Returns 0 on success, -1 if no node could be reached. */
static int send_command(int port, const char *cmd) {
    int hops = 0;
    while (hops++ < 8) {
        int fd = tcp_connect("127.0.0.1", port, 1000);
        if (fd < 0) {
            fprintf(stderr, "could not connect to port %d\n", port);
            return -1;
        }

        buf_t req;
        buf_init(&req);
        buf_put_str(&req, cmd, (uint16_t)strlen(cmd));
        int sent = send_msg(fd, MSG_CLIENT_REQUEST, req.data, (uint32_t)req.len);
        buf_free(&req);
        if (sent != 0) {
            close(fd);
            return -1;
        }

        uint8_t type;
        uint8_t *payload = NULL;
        uint32_t len = 0;
        int rc = recv_msg(fd, &type, &payload, &len);
        close(fd);
        if (rc != 0) {
            fprintf(stderr, "no response from node\n");
            return -1;
        }

        rdr_t rd;
        rdr_init(&rd, payload, len);
        if (type == MSG_REDIRECT) {
            uint32_t leader_port = 0;
            rdr_u32(&rd, &leader_port);
            free(payload);
            if (leader_port == 0 || (int)leader_port == port) {
                printf("no leader available, retry\n");
                return 0;
            }
            port = (int)leader_port; /* follow redirect */
            continue;
        } else if (type == MSG_CLIENT_RESPONSE) {
            uint8_t status = 0;
            char msg[256];
            uint16_t mlen = 0;
            rdr_u8(&rd, &status);
            rdr_str(&rd, msg, sizeof msg, &mlen);
            free(payload);
            printf("%s\n", msg);
            return 0;
        }
        free(payload);
        return -1;
    }
    printf("too many redirects\n");
    return 0;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    int port = -1;
    const char *oneshot = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            oneshot = argv[++i]; /* run a single command, then exit */
        }
    }
    if (port < 0) {
        fprintf(stderr, "usage: %s --port <p> [-c \"COMMAND\"]\n", argv[0]);
        return 2;
    }

    if (oneshot) return send_command(port, oneshot) == 0 ? 0 : 1;

    char line[512];
    printf("connected to port %d. commands: SET k v | GET k | DEL k\n", port);
    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(line, sizeof line, stdin)) break;
        size_t n = strlen(line);
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';
        if (n == 0) continue;
        if (strcmp(line, "quit") == 0 || strcmp(line, "exit") == 0) break;
        send_command(port, line);
    }
    return 0;
}
