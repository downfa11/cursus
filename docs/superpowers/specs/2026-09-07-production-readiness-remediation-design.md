# Production-readiness remediation design

## Status and scope

This design remediates the eighteen R01--R18 findings in
`docs/production-readiness-review-2026-09-07.md`. It applies to the
standalone broker, SDK, event-store path, Helm chart, and release test gate.
It deliberately changes the Producer delivery-result contract: a buffer drain
is not a successful delivery unless every batch has a matching broker ACK.

The work is one release-sized change, implemented and tested in the dependency
order below. A later stage must not weaken a correctness property established
by an earlier stage.

## Compatibility contract

`Producer.Flush` changes from `func()` to `func() error`. Direct calls that
discard its result remain valid Go statements, but function values and
interfaces using the old signature must be updated. `Producer.Close` keeps
its `error` signature and now returns a recorded permanent delivery failure
or outcome-unknown failure even when its buffers have drained.

`Send` and `PublishMessage` retain their local-acceptance meaning; their
sequence return is not a broker acknowledgement. A caller requiring delivery
confirmation checks `Flush` or `Close`. Every failed batch is classified
exactly once as permanent, retryable-exhausted, or outcome-unknown. The last
classification is used after incomplete I/O where the broker may have accepted
the batch, and is never reported as success.

The wire protocol gains a producer request correlation value. An ACK must echo
it with producer ID, epoch, partition, and inclusive sequence range. A client
discards an ACK that does not match its outstanding request. A timeout or
partial response closes the connection before retry.

`READ_STREAM` gains bounded pagination: `limit` and `max_bytes` bound a
request and a response contains a cursor when more data is available. The
server validates and encodes the entire response before writing success.
Existing small responses retain their current result shape.

## Storage, resource, and integrity design

### Ordered durable append (R01, R06, R07)

Each partition has one append sequencer which owns offset assignment, queue
order, physical writes, durable-tail advancement, and completion notification.
Async, sync, and sync-batch calls submit ordered append jobs; no public path
writes around queued work. Sync jobs wait for the completion covering their
last offset, which covers every earlier accepted job. A validated batch shares
one write-and-sync durability boundary.

A terminal write or fsync error fails the sequencer, rejects later appends,
records its cause in the disk runtime, and causes readiness to fail. Metrics
expose terminal state and filesystem headroom. Flush watermarks are monotonic
and only represent physically ordered durable records.

Application log records gain a payload checksum and recovery verifies it before
making a segment readable (R16). Existing segments follow an explicit format
compatibility rule; unsupported unchecksummed records are marked legacy rather
than silently treated as verified.

### Bounded read and transaction state (R08, R15, R17)

Consume and stream reads use broker-wide byte and request budgets, with
per-request record and byte limits. Reservations are released on completion,
cancellation, and connection close. Transaction visibility retains only open
transactions and retained-range-relevant metadata; completed entries are
pruned as retention advances. Visible count is incremental over the requested
window rather than rescanning prior results.

Transaction journal mutation records become per-transaction deltas. Snapshot
creation targets one transaction and has a bounded state image; prepare and
final decisions retain explicit fsync boundaries. Configuration defines maximum
active transactions, records, bytes, and journal growth, rejecting work before
unbounded allocation.

## SDK connection and lifecycle design

### Session factory and producer results (R02--R04)

Producer metadata, admin topic creation, and partition connections use one
context-aware session factory. It performs dialing, TLS handshake, protocol
negotiation, authentication, deadlines, and closure consistently. A configured
TLS connection cannot fall back to plaintext for bootstrap or admin commands.

The producer records terminal delivery states independently of buffer length.
`Flush` creates a barrier for all batches submitted before it, waits for
matching ACKs, and returns their aggregate delivery error. `Close` creates a
final barrier, stops senders only after it resolves or times out, closes
transport, and preserves its result for concurrent callers. Aggregate errors
retain individual ranges and classifications via `errors.Is`/`errors.As`.

### Consumer lifecycle and commit policy (R09, R14)

The Consumer/ConsumerGroup owns a root context for connections, heartbeat,
metadata refresh, and shutdown. Each assignment generation owns a child
context and generation-only wait group. Rebalance cancels and waits only for
the old generation workers, then starts the next one; it never waits for root
workers. `Start` waits for root shutdown and `Close` cancels root context
once and waits idempotently.

Partition workers commit automatically only when `EnableAutoCommit` is true.
When false, processing completion only advances local state; explicit commit is
the only request path. Tests cover repeated rebalance, handler failure, worker
leaks, and manual-commit restart behavior.

## Transaction recovery design (R10)

New transaction requests remain fenced by the current consumer-group
generation. Once prepare is durable, recovery is authorized by durable
transaction identity and decision state, not historical-generation
revalidation. The state machine is:

`open -> prepared -> applying -> committed | aborted`.

Every durable transition stores idempotency data to retry it. Recovery of
`prepared` aborts before irreversible application or resumes a recorded
decision. Recovery of `applying` completes the durable decision, output
visibility markers, and offset application in the documented order; it cannot
remain indefinitely unabortable. Replaying a completed action is a no-op.
Recovery exposes a terminal failed state and readiness/metrics signal if safe
convergence is impossible.

Fault-injection tests stop the process after each persistence boundary, expire
or replace the consumer member, restart coordinator and storage, then assert
one converged output/offset result and readable `read_committed` data.

## Event sourcing design (R11, R12)

The stream index is a rebuildable cache backed by retained log records plus a
durable stream-version checkpoint or snapshot. Rebuild starts at
`GetFirstOffset` and uses the checkpoint to establish version before the
retained range. If retention removed required history and no trustworthy
checkpoint exists, opening the index fails with a recovery error; it never
substitutes version zero. Topic policy rejects delete retention for event topics
unless the checkpoint capability is enabled.

Read-stream pagination is record-and-byte bounded and includes a next cursor
only after a complete, size-validated payload is ready. The SDK reads pages
until the requested limit or cursor boundary. Tests cover restarted indexes
after retention, entirely removed aggregates, non-one first retained versions,
and payloads exceeding one frame.

## Server and deployment safety (R05, R13, R18)

If a read deadline fires after any byte of a frame has been consumed, the
server closes that connection. It treats only a timeout before frame input as
idle poll, so remaining payload cannot be interpreted as a new length prefix.

Long poll validates an upper `wait_ms` bound and waits on the minimum of that
duration, request context cancellation, and broker shutdown context. Shutdown
propagates one deadline through handlers and storage drain, allowing Helm grace
period to be based on a measured upper bound.

The single-replica Helm chart uses a `Recreate` strategy and an
application-level exclusive lock on the data directory. Lock contention fails
startup/readiness with a clear diagnostic. The chart documents standalone
storage, not HA replication, and supplies explicit operational resource,
security, probe, and termination-grace values.

## Observability, operational artifacts, and release gates

The chart supplies PrometheusRule resources for terminal storage failure,
filesystem headroom, fsync failures/latency, unresolved transactions,
LEO/HWM/LSO divergence, rebalance stalls, consumer lag/out-of-range, request
timeouts, budget rejection, and process memory/GC. Labels avoid unbounded
producer, topic, and group cardinality.

Operations documentation specifies an offline-consistent backup procedure for
manifest, consumer metadata, transaction journal, segments, checkpoints, and
snapshots. A restore drill verifies payloads, offsets, transaction visibility,
ACLs, and stream versions, and documents rollout/rollback limits and supported
format migration.

The E2E workflow path filter includes `sdk/**`. The release gate runs unit,
race, storage fault, SDK ACK/retry, mixed-ack restart, transaction generation,
event retention/replay, long-poll shutdown, and shared-volume rollout
scenarios. Linux Docker/Kubernetes jobs are required for release approval.
Benchmarks publish workload-specific p99, sync, allocation, and recovery
measurements rather than claiming fixed capacity.

## Implementation order and acceptance criteria

1. Add regression tests and ordered append/terminal-readiness contract.
2. Add session factory, correlated ACKs, delivery barriers, and migrate SDK
   callers to checked `Flush`/ `Close` errors.
3. Separate consumer root/generation lifecycle and enforce manual commit.
4. Implement durable transaction recovery transitions and fault tests.
5. Implement checkpointed event-index recovery and bounded stream pagination.
6. Apply framing, budget, cancellation, checksum, visibility cleanup, and
   transaction-delta changes.
7. Add Helm locking/Recreate policy, alerts, operational profiles/runbooks,
   and CI release gates.

Completion requires every R01--R18 regression to pass without overlays,
`go test ./pkg/... ./sdk/... ./util/...` to pass, `go test -race` on affected
packages to pass, and applicable Docker/Kubernetes E2E jobs to pass. No
completion claim is made for unmeasured throughput, RPO, or RTO; those are
published only from release-gate measurements.

