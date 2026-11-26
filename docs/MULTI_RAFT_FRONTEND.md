# Multi-Raft Frontend and Demonstration Guide

## Overview

The ShardRAFT Multi-Raft dashboard connects to the actual
five-node distributed database through local HTTP interfaces.

The original three-node development mode remains available.

## Deployment

| Component | Configuration |
|---|---|
| Storage nodes | 5 |
| HTTP gateways | 2 |
| Data Raft groups | 4 |
| Metadata Raft groups | 1 |
| Replication factor | 3 |
| Logical hash slots | 65,536 |
| Dashboard pages | 15 |

## Prerequisites

- Docker Desktop or Docker Engine with Compose.
- Go version specified in go.mod.
- Node.js and npm.
- Available local service ports.

## Startup

Run the following commands from the repository root.

### Terminal 1: Database

```bash
docker compose up -d --build --wait
docker compose ps
```

For an existing deployment with current images:

```bash
docker compose up -d --no-build --wait
```

### Terminal 2: Docker Control

```bash
go run ./cmd/shardraft-docker-control
```

Keep this terminal running.

### Terminal 3: Frontend

```bash
cd web
npm ci
npm run dev
```

Keep this terminal running.

Open the Multi-Raft dashboard:

http://127.0.0.1:5173/?mode=multi

## Local Endpoints

| Component | Endpoint |
|---|---|
| Dashboard | http://127.0.0.1:5173/?mode=multi |
| Docker control health | http://127.0.0.1:18090/health |
| Node 1 | http://127.0.0.1:18081/status |
| Node 2 | http://127.0.0.1:18082/status |
| Node 3 | http://127.0.0.1:18083/status |
| Node 4 | http://127.0.0.1:18084/status |
| Node 5 | http://127.0.0.1:18085/status |
| Gateway 1 | http://127.0.0.1:18091 |
| Gateway 2 | http://127.0.0.1:18092 |

## Demonstration Sequence

### 1. Overview and Architecture

Open Overview and confirm five reachable storage nodes.

Inspect the four independent data groups and replicated
metadata group.

Open Architecture to explain the client, gateway, metadata,
Raft replication, and persistent storage layers.

### 2. Sharding and Metadata

Open Shards and inspect the configured placement.

| Shard | Slot range | Replicas |
|---|---|---|
| S1 | 0-16383 | n1, n2, n3 |
| S2 | 16384-32767 | n2, n3, n4 |
| S3 | 32768-49151 | n3, n4, n5 |
| S4 | 49152-65535 | n4, n5, n1 |

Metadata replicas are hosted by n1, n2, and n3.

Enter a key into the routing explorer and inspect its
CRC32 hash slot and owning shard.

### 3. Live Database Operations

Open Query Console.

Use a fresh demonstration key to avoid existing data.

| Operation | Gateway | Input | Expected result |
|---|---|---|---|
| PUT | 1 | hello | applied=true |
| GET | 2 | Same key | hello |
| CAS | 2 | expected=hello, value=updated | applied=true |
| CAS | 1 | expected=incorrect, value=other | applied=false |
| GET | 1 | Same key | updated |

Inspect the response, request identity, gateway attempts,
routing preview, and request history.

Keep the key temporarily for the remaining demonstration.

### 4. Consensus and Replication

Open Cluster and Replication.

Inspect the owning shard's current leader, term, commit
index, applied index, and configured replicas.

Each data group elects its own leader independently.

### 5. Persistent Storage

Open Raft Log, Database, and Storage.

Select the shard and one of its configured replicas.

Inspect the replicated log, locally applied records,
WAL diagnostics, and available snapshot information.

These are local observations and may temporarily differ
between replicas.

### 6. Transport and Observability

Open Transport, Metrics, and Events.

Inspect peer communication statistics, sampled cluster
observations, and shard-tagged event history.

The event viewer uses bounded polling rather than a
lossless distributed tracing system.

### 7. Live Fault Injection

Use only the local demonstration cluster.

Before starting:

- Confirm all five storage nodes are reachable.
- Confirm the selected shard has three reachable replicas.
- Identify its current leader.
- Prefer a data leader that is not the metadata leader.
- Confirm no other fault experiment is running.

In Fault Lab, stop one selected storage node.

Observe the replacement leader and the remaining replicas.

After a replacement leader is observed, return to Query Console.

Read the existing demonstration key, update its value,
and read the updated value through the other gateway.

Return to Fault Lab and start the stopped node.

Inspect replica recovery in Replication.

Verify that the recovered replica catches up to the
observed commit and applied indexes.

Confirm all five storage nodes are reachable again.

The Docker controls perform real local container lifecycle
operations. A graceful stop is not equivalent to a
power-loss crash.

### 8. Correctness and Performance

Open Test Lab.

Explain the recorded Porcupine single-key linearizability
results and the scope of the historical evidence.

Open Performance Lab.

Compare throughput, latency percentiles, fault measurements,
and isolated WAL synchronization costs.

Recorded evaluation results:

- Eight distributed workload experiments.
- 8,000 successful operations.
- Eight linearizable recorded histories.
- Peak observed throughput of 1,483.75 operations per second.
- 30,000 isolated WAL microbenchmark records.

The dashboard displays historical results and does not
execute new distributed benchmarks or checker runs.

### 9. Cleanup

Return to Query Console.

Delete the demonstration key and confirm a subsequent
GET reports that the key is missing.

Confirm that all five storage containers have recovered.

## Browser Validation

Seven Multi-Raft browser tests passed across focused runs.

| Validation area | Result |
|---|---|
| Storage and metadata availability | Passed |
| All fifteen dashboard pages | Passed |
| Historical evaluation evidence | Passed |
| Eight-operation KV lifecycle | Passed |
| Gateway retry identity preservation | Passed |
| Reload persistence and deduplication | Passed |
| Actual leader failure and replica recovery | Passed |

### Recorded Browser Failover

The validated experiment stopped Node 2, the initial
leader of Shard 2.

Node 3 became leader, and the term increased from 4 to 5.

| Measurement | Result |
|---|---|
| Observed stop-to-election interval | 3165 ms |
| Observed recovery interval | 1463 ms |
| Recovered commit index | 945 |
| Recovered applied index | 945 |
| Committed value preservation | Passed |
| Post-failover reads and writes | Passed |
| Final storage health | 5/5 healthy |

These timings include control and polling overhead.
They are not precise consensus-protocol latency measurements.

## Troubleshooting

### Dashboard Cannot Reach Storage Nodes

```bash
docker compose ps -a
```

Confirm the storage services are running.

### Docker Controls Are Unavailable

```bash
curl http://127.0.0.1:18090/health
```

If the service is not running, start it from the repository root:

```bash
go run ./cmd/shardraft-docker-control
```

### A Storage Node Remains Stopped

Inspect the deployment and start the specific stopped node.

For example, to restore Node 2:

```bash
docker compose start n2
docker compose ps -a
```

Allow time for its health check and replica catch-up.

### Proxy Errors During Fault Injection

Temporary connection-refused and socket errors are expected
while a deliberately stopped node is unavailable or restarting.

Persistent errors after recovery require investigation.

### Wrong Dashboard Mode

Open the explicit Multi-Raft URL:

http://127.0.0.1:5173/?mode=multi

The original development mode uses the root URL and
requires its separate development backend.

## Shutdown

Stop the frontend and Docker control service using Ctrl+C
in their respective terminals.

Stop the Docker deployment:

```bash
docker compose stop
```

Named storage volumes remain intact.

## Limitations

- Fixed physical shard placement.
- No dynamic shard migration or rebalancing.
- No multi-key distributed transactions.
- Local diagnostics are not linearizable reads.
- Reachability indicators are not proofs of quorum.
- Event and metrics observations are bounded.
- Unresolved mutations use same-tab session storage.
- Query history and fault history are not persistent.
- Historical evaluation pages do not launch live experiments.
- Browser gateway failure injection is not a gateway process crash.
- Docker fault injection uses graceful container stops.
- Docker control is intended for trusted local use only.

## Related Documentation

- [Project README](../README.md)
- [Docker Deployment](DEPLOYMENT.md)
- [Multi-Raft Acceptance](MULTI_RAFT_ACCEPTANCE.md)
- [Performance Evaluation](PERFORMANCE.md)
- [HTTP Gateway](HTTP_GRPC_GATEWAY.md)
- [Sharding](SHARDING.md)
