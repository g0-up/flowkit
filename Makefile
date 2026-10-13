GO            ?= go
TOOLS         := $(GO) tool -modfile=tools/go.mod
COMPOSE       := docker compose -f docker-compose.test.yml
# Test-only DSN for the local container from docker-compose.test.yml; override
# by exporting FLOWKIT_TEST_DATABASE_URL.
export FLOWKIT_TEST_DATABASE_URL ?= postgres://flowkit:flowkit@localhost:55432/flowkit_test?sslmode=disable

.PHONY: test lint sqlc sqlc-check pg-up pg-down freeze corpus chaos nfr

# Run `make pg-up` first: these targets point the Postgres-backed tests at the
# local container. Plain `go test ./...` without the variable skips them.
test:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...
	$(TOOLS) staticcheck ./...

# Regenerates internal/store/db from internal/store/queries and the migrations.
sqlc:
	$(TOOLS) sqlc generate

# Fails when the generated code differs from what sqlc would generate now.
# sqlc diff compares against the files on disk, so it also works before the
# generated code is committed.
sqlc-check:
	$(TOOLS) sqlc diff

# The public API against api/freeze-v1.txt, reserved names, job kinds and
# queues, and the guard keeping test hooks out of production code.
freeze:
	$(GO) test -count=1 -run '^TestFreeze' .
	$(GO) test -count=1 ./internal/testhooks

# Replays testdata/corpus on a real Engine (needs `make pg-up`).
corpus:
	$(GO) test -race -count=1 -run '^TestCorpus' .

# Kills real worker processes at every crash point, 50 rounds each (needs
# `make pg-up`; about two minutes). Plain `go test` skips it.
chaos:
	FLOWKIT_CHAOS=1 $(GO) test -race -count=1 -timeout 20m ./internal/chaos

# Measures wake latency and timer lag with the real clock (needs `make pg-up`;
# about two minutes). Results are recorded in docs/runbook-engine.md.
nfr:
	FLOWKIT_NFR=1 $(GO) test -count=1 -v -timeout 15m -run TestLatencyBudgets ./internal/nfr

pg-up:
	$(COMPOSE) up -d --wait

pg-down:
	$(COMPOSE) down -v
