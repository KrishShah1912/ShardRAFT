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
