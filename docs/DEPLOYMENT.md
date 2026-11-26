# ShardRAFT Local Docker Deployment

## Overview

ShardRAFT supports a local Docker Compose deployment
consisting of five storage nodes and two HTTP gateways.

All services use a shared multi-stage Docker image.
The runtime executes as a non-root user with UID 10001.

## Requirements

- Docker Engine
- Docker Compose
- Go version specified in go.mod

## Deployment

From the repository root:

```bash
docker compose up -d --build --wait
docker compose ps
```

## Storage topology

| Group | Slots | Replicas |
|---|---|---|
| S1 | 0-16383 | n1, n2, n3 |
| S2 | 16384-32767 | n2, n3, n4 |
| S3 | 32768-49151 | n3, n4, n5 |
| S4 | 49152-65535 | n4, n5, n1 |
| Metadata | N/A | n1, n2, n3 |

Each storage node has an independent named volume
mounted at /data.

## Service endpoints

| Service | Endpoint |
|---|---|
| n1 | http://127.0.0.1:18081/status |
| n2 | http://127.0.0.1:18082/status |
| n3 | http://127.0.0.1:18083/status |
| n4 | http://127.0.0.1:18084/status |
| n5 | http://127.0.0.1:18085/status |
| gateway1 | http://127.0.0.1:18091 |
| gateway2 | http://127.0.0.1:18092 |

Raft and metadata communication use internal Docker
network addresses on gRPC port 9090.

## Example operations

Write through gateway1:

```bash
curl -X PUT http://127.0.0.1:18091/kv/example \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello","client_id":"demo","request_id":"write-1"}'
```

Read through gateway2:

```bash
curl http://127.0.0.1:18092/kv/example
```

Read through the internal gRPC client:

```bash
docker compose exec -T n1 shardraft \
  --grpc-endpoints=n1:9090,n2:9090,n3:9090 \
  get example
```

## Chaos control

Build the host-side CLI:

```bash
mkdir -p bin
go build -o bin/chaosctl ./cmd/chaosctl
```

Available commands:

```bash
./bin/chaosctl status
./bin/chaosctl stop n1
./bin/chaosctl start n1
./bin/chaosctl restart n1
```

Run these commands from the repository root.

## Integration and fault tests

Docker-dependent tests are opt-in.

```bash
SHARDRAFT_DOCKER_TEST=1 go test -v ./test \
  -run '^TestDockerMultiShardIntegration$' \
  -count=1 -timeout=90s
```

```bash
SHARDRAFT_DOCKER_TEST=1 go test -v ./test \
  -run '^TestDocker(LeaderFailover|FollowerLoss)$' \
  -count=1 -timeout=240s
```

The cluster must already be running.

## Validated behavior

- Five healthy storage nodes and two running gateways.
- Five independent persistent storage volumes.
- Correct routing across all four data shards.
- Cross-gateway PUT and linearizable GET.
- Duplicate request handling.
- CAS success and mismatch handling.
- DELETE and shard isolation.
- Leader replacement following container stop.
- Preservation of committed values after failover.
- Duplicate request retry following leader replacement.
- Continued writes during follower loss.
- Replica catch-up following container restart.
- Repository build and static analysis.

## Validation observations

The local validation run observed:

| Test | Result |
|---|---|
| Multi-shard integration | PASS (0.15s) |
| Leader failover | PASS (3.27s) |
| Follower loss | PASS (2.55s) |
| Observed election wait | 1.746s |

During the leader failure test, S1 leadership moved
from node 1 in term 1 to node 3 in term 2.

The restarted node caught up to commit and applied
index 6. During the follower recovery test, it
subsequently caught up to index 8.

These are observations from one local validation run,
not formal performance benchmark results.

## Stop and resume deployment

Stop the deployment while preserving named volumes:

```bash
docker compose stop
```

Resume the deployment:

```bash
docker compose up -d --no-build --wait
```

Remove containers and the network while retaining
named volumes:

```bash
docker compose down
```

## Limitations

- This deployment is intended for local development
  and testing, not production use.
- Storage health checks verify daemon responsiveness,
  not Raft quorum availability.
- Gateways do not expose dedicated health endpoints.
- Fault tests cover graceful container stops and
  restarts, not abrupt crashes or power loss.
- Replica catch-up is verified through commit and
  applied indexes.
- Independent disk-only recovery is not established
  by the Docker fault tests.
- Fault tests exercise shard S1.
- Integration tests retain their generated records.
- Formal performance measurements are deferred to
  Step 10.
