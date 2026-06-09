CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra -std=c11 -pthread
LDFLAGS ?= -pthread

NODE_SRC   = src/raft.c src/log.c src/net.c src/kv.c src/timer.c
CLIENT_SRC = src/client.c src/net.c

.PHONY: all clean test test-tsan test-asan

all: raft-kv raft-client

raft-kv: src/main.c $(NODE_SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

raft-client: $(CLIENT_SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# --- sanitizer builds (separate binary names so tests can target them) ---
raft-kv-tsan: src/main.c $(NODE_SRC)
	$(CC) $(CFLAGS) -fsanitize=thread -o $@ $^ $(LDFLAGS)

raft-kv-asan: src/main.c $(NODE_SRC)
	$(CC) $(CFLAGS) -fsanitize=address,undefined -o $@ $^ $(LDFLAGS)

test: all
	@for t in tests/test_basic.sh tests/test_leader_failure.sh tests/test_chaos.sh; do \
		echo "=== $$t ==="; \
		sh $$t || exit 1; \
	done

test-tsan: raft-kv-tsan raft-client
	BIN=./raft-kv-tsan sh tests/test_basic.sh
	BIN=./raft-kv-tsan sh tests/test_leader_failure.sh

test-asan: raft-kv-asan raft-client
	BIN=./raft-kv-asan sh tests/test_basic.sh
	BIN=./raft-kv-asan sh tests/test_leader_failure.sh

clean:
	rm -f raft-kv raft-client raft-kv-tsan raft-kv-asan
	rm -f log_*.bin meta_*.bin
	rm -rf testdata
