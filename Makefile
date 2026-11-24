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
