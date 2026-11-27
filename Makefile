.PHONY: build test race vet fmt

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')
.PHONY: dev-cluster stop-cluster cluster-status reset-cluster

dev-cluster:
	@./scripts/dev-cluster.sh

stop-cluster:
	@./scripts/stop-cluster.sh

cluster-status:
	@./scripts/cluster-status.sh

reset-cluster: stop-cluster
	@rm -rf .dev-cluster
	@echo "Development cluster data removed."

.PHONY: dev-web

dev-web:
	@cd web && npm run dev

.PHONY: stop-node start-node restart-node

stop-node:
	@if [ -z "$(NODE)" ]; then \
		echo "Usage: make stop-node NODE=1"; \
		exit 1; \
	fi
	@./scripts/node-control.sh stop "$(NODE)"

start-node:
	@if [ -z "$(NODE)" ]; then \
		echo "Usage: make start-node NODE=1"; \
		exit 1; \
	fi
	@./scripts/node-control.sh start "$(NODE)"

restart-node:
	@if [ -z "$(NODE)" ]; then \
		echo "Usage: make restart-node NODE=1"; \
		exit 1; \
	fi
	@./scripts/node-control.sh restart "$(NODE)"

.PHONY: dev-supervisor

dev-supervisor:
	@go run ./cmd/shardraft-supervisor \
		-root . \
		-addr 127.0.0.1:8090

.PHONY: dev

dev:
	@./scripts/dev.sh

.PHONY: validation

validation:
	cd web && npm run validation:generate


.PHONY: validation-standard

validation-standard:
	cd web && npm run validation:standard

.PHONY: frontend-build frontend-test verify

frontend-build:
	cd web && npm run build

frontend-test:
	cd web && npm test

verify: build vet test frontend-build frontend-test
	@echo
	@echo "ShardRAFT verification passed."

.PHONY: multi-up multi-down multi-status multi-control multi-web

multi-up:
	docker compose up -d --build

multi-down:
	docker compose down

multi-status:
	docker compose ps

multi-control:
	go run ./cmd/shardraft-docker-control \
		-addr 127.0.0.1:18090

multi-web:
	cd web && npm run dev

.PHONY: demo demo-down

demo:
	@./scripts/demo.sh

demo-down:
	@./scripts/demo-down.sh
