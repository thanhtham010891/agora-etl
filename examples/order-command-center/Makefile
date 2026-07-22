VENV ?= .venv
BOOTSTRAP_PYTHON ?= $(or $(shell command -v python3.13 2>/dev/null),$(shell command -v python3.12 2>/dev/null),$(shell command -v python3.11 2>/dev/null),$(shell command -v python3 2>/dev/null))
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
SETUP_STAMP := $(VENV)/.setup-complete
STACK_STATE_DIR := .demo-state
STACK_FINGERPRINT_FILE := $(STACK_STATE_DIR)/compose-inputs.sha256
COMPOSE_INPUTS := compose.yaml .dockerignore Dockerfile pyproject.toml src sql observability $(wildcard .env)
CRASH_GROUP := order-postgres-crash-$(shell date +%s)
ORDER_COUNT ?= 1000
KAFKA_FLUSH_EVERY ?= 100
ORDER_EVENT_VERSION ?= 1
VERIFY_WAIT_SECONDS ?= 0
SMOKE_WAIT_SECONDS ?= 120

.DEFAULT_GOAL := help

.PHONY: help setup up stop down status logs migrate producer verify smoke drill-crash drill-worker-restart convergence dlq dlq-replay dlq-reconcile reconcile test test-integration test-release-gate

##@ General
help: ## Show this command reference.
	@awk 'BEGIN { FS = ":.*##"; printf "Order Command Center\n\nUsage: make <target>\n" } /^##@/ { printf "\n%s\n", substr($$0, 5); next } /^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-24s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

##@ Environment
setup: $(SETUP_STAMP) ## Create or refresh the local virtual environment.

$(SETUP_STAMP): pyproject.toml
	@test -n "$(BOOTSTRAP_PYTHON)" || (echo "Python 3.11+ is required; install python3 first." && exit 1)
	@test -x "$(PYTHON)" || $(BOOTSTRAP_PYTHON) -m venv $(VENV)
	$(PIP) install -e ".[dev]"
	@touch $(SETUP_STAMP)

##@ Local stack
up: setup ## Start infrastructure, migrations, and persistent projection workers.
	@fingerprint="$$(find $(COMPOSE_INPUTS) -type d -name __pycache__ -prune -o -type f -print | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | awk '{print $$1}')"; \
	expected_services="$$(docker compose config --services | grep -vx 'migrate')"; \
	running_services="$$(docker compose ps --status running --services 2>/dev/null || true)"; \
	missing_service="$$(printf '%s\n' "$$expected_services" | while IFS= read -r service; do \
		[ -z "$$service" ] || printf '%s\n' "$$running_services" | grep -qx "$$service" || { printf '%s' "$$service"; break; }; \
	done)"; \
	if [ -z "$$missing_service" ] && [ ! -f "$(STACK_FINGERPRINT_FILE)" ]; then \
		mkdir -p "$(STACK_STATE_DIR)"; \
		printf '%s\n' "$$fingerprint" > "$(STACK_FINGERPRINT_FILE)"; \
	elif [ -n "$$missing_service" ] || [ "$$(cat "$(STACK_FINGERPRINT_FILE)" 2>/dev/null)" != "$$fingerprint" ]; then \
		docker compose up -d --wait --build && \
		mkdir -p "$(STACK_STATE_DIR)" && \
		printf '%s\n' "$$fingerprint" > "$(STACK_FINGERPRINT_FILE)"; \
	fi

stop: ## Stop services while preserving local volumes.
	docker compose stop

down: ## Remove the local stack and all demo volumes.
	docker compose down --volumes

status: ## Show infrastructure and projection-worker status.
	docker compose ps

logs: ## Follow PostgreSQL and Redis projection-worker logs.
	docker compose logs -f --tail=100 postgres-worker redis-worker

migrate: up ## Apply versioned PostgreSQL projection migrations.
	$(PYTHON) -m order_command_center.migrate

##@ Pipeline operations
verify: migrate ## Check the latest published batch has converged in PostgreSQL and Redis.
	$(PYTHON) -m order_command_center.verify --wait-seconds $(VERIFY_WAIT_SECONDS)

producer: migrate ## Publish generated order lifecycles to Kafka.
	$(PYTHON) -m order_command_center.producer --orders $(ORDER_COUNT) --flush-every $(KAFKA_FLUSH_EVERY) --event-version $(ORDER_EVENT_VERSION)

smoke: producer ## Publish generated orders and wait for PostgreSQL and Redis convergence.
	$(MAKE) verify VERIFY_WAIT_SECONDS=$(SMOKE_WAIT_SECONDS)

drill-crash: migrate ## Prove replay after a sink-flush-before-acknowledgement crash.
	rm -rf .demo-state
	@echo "Crash drill consumer group: $(CRASH_GROUP)"
	POSTGRES_GROUP=$(CRASH_GROUP) $(PYTHON) -m order_command_center.pipelines.postgres --max-records 1 --hard-crash-after-flush || test $$? -eq 75
	@test -f .demo-state/postgres-flushed.marker || (echo "No Kafka record was available for the crash drill. Run 'make producer' first." && exit 1)
	POSTGRES_GROUP=$(CRASH_GROUP) $(PYTHON) -m order_command_center.pipelines.postgres --max-records 9
	$(PYTHON) -m order_command_center.verify

drill-worker-restart: up ## Prove both projections converge after a worker crash.
	AGORA_RUN_INTEGRATION=1 $(PYTHON) -m pytest tests/integration/test_projection_recovery.py::test_worker_restart_converges_consumer_groups_and_projections -q

convergence: up ## Inspect committed offsets and lag for both projection groups.
	$(PYTHON) -m order_command_center.operations.convergence

##@ DLQ and replay
dlq: setup ## List poison records and replay state.
	$(PYTHON) -m order_command_center.dlq list

dlq-replay: migrate ## Validate or execute a ticketed corrected replay.
	@test -n "$(DLQ_ID)" || (echo "Set DLQ_ID=<record id>." && exit 1)
	@test -n "$(PAYLOAD_FILE)" || (echo "Set PAYLOAD_FILE=<corrected event json>." && exit 1)
	@test -n "$(CHANGE_TICKET)" || (echo "Set CHANGE_TICKET=<approved change or incident>." && exit 1)
	@test -n "$(REPLAY_REASON)" || (echo "Set REPLAY_REASON=<why this correction is safe>." && exit 1)
	$(PYTHON) -m order_command_center.dlq replay $(DLQ_ID) --payload-file $(PAYLOAD_FILE) --ticket $(CHANGE_TICKET) --reason "$(REPLAY_REASON)" $(if $(EXECUTE),--execute)

dlq-reconcile: migrate ## Reconcile a stranded replay only after ledger proof.
	@test -n "$(REPLAY_ID)" || (echo "Set REPLAY_ID=<publishing replay id>." && exit 1)
	$(PYTHON) -m order_command_center.dlq reconcile $(REPLAY_ID)

reconcile: migrate ## Finalize a producer manifest after ledger evidence.
	@test -n "$(PRODUCER_RUN_ID)" || (echo "Set PRODUCER_RUN_ID=<id> from producer output." && exit 1)
	$(PYTHON) -m order_command_center.reconcile $(PRODUCER_RUN_ID)

##@ Verification
test: setup ## Run fast unit and contract tests.
	$(PYTHON) -m pytest tests -q

test-integration: up ## Run Docker-backed recovery and rebuild contracts.
	AGORA_RUN_INTEGRATION=1 $(PYTHON) -m pytest tests/integration -q

test-release-gate: test test-integration ## Run the full local release gate.
