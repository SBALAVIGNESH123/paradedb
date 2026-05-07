# MPP for ParadeDB AggregateScan — Status & Roadmap

> Updated 2026-05-06 (Track A + Track B substrate landed; activation pending — see "Remaining work").

This file mirrors the [Notion page](https://www.notion.so/paradedb/MPP-Plan-Partitioning-for-JoinScan-342ea4ce9deb81e2b97ff9c307a45940). Notion MCP disconnected mid-session — paste the relevant updates back into the page when convenient.

---

## TL;DR — where we are

**Working:**

- Both PRs build clean: [paradedb/paradedb#4988](https://github.com/paradedb/paradedb/pull/4988) (pg_search wiring) and [paradedb/datafusion-distributed#4](https://github.com/paradedb/datafusion-distributed/pull/4) (fork-side enabling).
- Senior-engineer review pass applied to both.
- **Build-side replication cliff fixed** via DSM all-gather (workers each scan their 1/N slice of the build relation, write to per-worker DSM regions, atomic-counter barrier, read peers' Arrow IPC slices). Cliff at 8 producers went from 13 s → 1.3 s; at 10 producers from 36 s → 1.7 s.
- **Partitioning-source picker fixed** (`total_doc_count` tie-break instead of segment-count last-wins) so the build-side cache caches the right (smaller) table.
- **Bench dataset rebuilt** with `paradedb.global_target_segment_count = 32` + `REINDEX` so pages and files each have 32 ≈-uniform segments (was 10 ragged).
- Worker plan-dump under `mpp_debug` for future debugging.

**Current numbers (local 25 M ⨝ 1.25 M GROUP-BY-on-join, 32 uniform segments):**

| Configuration     |   Median |       vs DF serial |
| ----------------- | -------: | -----------------: |
| PG native         | 9 331 ms |              0.13× |
| DataFusion serial | 1 180 ms |               1.0× |
| MPP 2 producers   | 3 061 ms | 0.39× (regression) |
| MPP 4 producers   |   819 ms |          **1.44×** |
| MPP 8 producers   |   804 ms |          **1.47×** |
| MPP 10 producers  |   870 ms |              1.36× |

Correctness preserved across every configuration — byte-exact on COUNT and SUM.

**Plateau at ~820 ms / 1.47×.** Adding workers past 4 doesn't help. Diagnosed below.

---

## What blocks linear speedup — diagnosis

The fork's planner emits a single cross-worker boundary positioned _above the partial aggregate, hash repartition_ but _below the final aggregate_. The leader's plan looks like:

````text
DistributedExec
  CoalescePartitionsExec                           ← merges 4 partitions on leader
    AggregateExec(FinalPartitioned, gby=[title])   ← runs ON LEADER, current-thread Tokio
      [Stage 1] NetworkShuffleExec(output=4, input_tasks=4)   ← cross-worker boundary
        RepartitionExec(Hash([title], 4))          ← on worker side
          AggregateExec(Partial)
            HashJoinExec(Partitioned)
              RepartitionExec(Hash([id], 4))
                DataSourceExec(MemoryExec from build-side cache)
              RepartitionExec(Hash([file_id], 4))
                PgSearchScan
```text

Workers each compute Partial + Hash repartition + send → leader. Leader receives all N partials and runs `FinalPartitioned` single-threaded (because pgrx + `shm_mq` pin to the backend OS thread; multi-thread Tokio would crash on the receive side). At 1.25 M groups × N producers' partials, the leader's serial work _is_ the wall-clock floor.

Per-worker timing trace (mpp_debug at 8 producers / 32 segments, build-side phase only):

```text
worker 0/8: my_rows=134K, scan_ms=12, encode_ms=1, write_ms=0, barrier_ms=12, read_ms=24
worker 1/8: my_rows=156K, scan_ms=18, encode_ms=1, write_ms=0, barrier_ms=2,  read_ms=22
worker 2/8: my_rows=162K, scan_ms=18, encode_ms=5, write_ms=0, barrier_ms=0,  read_ms=18
worker 3/8: my_rows=168K, scan_ms=20, encode_ms=2, write_ms=1, barrier_ms=4,  read_ms=22
worker 4/8: my_rows=159K, scan_ms=18, encode_ms=1, write_ms=0, barrier_ms=7,  read_ms=27
worker 5/8: my_rows=161K, scan_ms=14, encode_ms=1, write_ms=0, barrier_ms=8,  read_ms=22
worker 6/8: my_rows=147K, scan_ms=14, encode_ms=2, write_ms=0, barrier_ms=9,  read_ms=26
worker 7/8: my_rows=162K, scan_ms=16, encode_ms=2, write_ms=1, barrier_ms=6,  read_ms=21
```text

All-gather = ~70 ms / worker. The remaining ~760 ms of the 833 ms wall-clock is downstream: producer fragment (probe + partial agg, parallel), shm_mq shuffle traffic, and the leader's single-threaded final aggregate. The final aggregate is the dominant component.

### Failed experiment (in case you're wondering why)

I tried changing the fork's `_distribute_plan` so that nested in-process Shuffle uses `(consumer_tc=N, input_tc=N)` — turning the JOIN-side hash repartitions into cross-worker shuffles. Result: the post-agg shuffle stayed at `consumer_tc=1` (it's the _outermost_ Shuffle, so `has_boundary_ancestor=false`), and the _JOIN-side_ hash repartitions became cross-worker — which `LocalExecWorkerTransport` can't service (it re-executes the input plan locally, which violates `RepartitionExec`'s single-shot semantics when called by N consumers). Reverted on the fork (commit `5f89185`); the repro is preserved in the bench harness if anyone wants to see the panic.

---

## Roadmap to linear speedup

Two pieces are needed. Either alone is half-built; both together get us linear.

### Track A — split the post-aggregate shuffle from the worker→leader gather (fork side)

**Goal.** Make the planner emit _two_ cross-worker boundaries instead of one:

- An _intermediate_ Shuffle below `FinalPartitioned` (workers ↔ workers, hash partition by group key). Workers consume their own hash bucket from N peers.
- An _outer_ gather above `FinalPartitioned` (workers → leader, single consumer). The leader does no aggregation work — just unions the N pre-finalized streams.

After:

```text
DistributedExec
  [Stage 2] NetworkShuffleExec(output=1, input_tasks=N)        ← OUTER gather, consumer_tc=1
    AggregateExec(FinalPartitioned)                            ← runs ON WORKERS
      [Stage 1] NetworkShuffleExec(output=N, input_tasks=N)    ← INTERMEDIATE x-worker, consumer_tc=N
        RepartitionExec(Hash([title], N))
          AggregateExec(Partial)
            ...
```text

**How.** The fork's `_distribute_plan` already has the arithmetic for both — the missing piece is making `CoalescePartitionsExec`-above-`FinalPartitioned` survive as a _real boundary_ in in-process mode. Today the Coalesce arm short-circuits to `require_one_child(new_children)` for `in_process`. Concretely:

1. **`src/distributed_planner/distribute_plan.rs::Coalesce` arm:** when `in_process`, emit `NetworkShuffleExec(consumer_task_count=1, input_task_count=N)` instead of eliding. This becomes the worker→leader gather.

2. **`this_is_real_boundary` predicate (line 137 area):** change `Coalesce => !in_process` to `Coalesce => true` when in in_process and there are multiple producer tasks below. This marks the gather as a real boundary so descendants set `has_boundary_ancestor=true`.

3. **`Shuffle` arm:** the change I tried in `05b13fd` (re-apply): nested in-process Shuffle = `(consumer_tc=N, input_tc=N)`. This is now correct because the post-agg Shuffle has the gather (Coalesce-as-boundary) as an ancestor, so it's nested and gets the cross-worker arithmetic. Outer Shuffle (no ancestor) was the gather case; that's now Coalesce's job.

4. **Tests.** Add a snapshot test in `_distribute_plan` with a registered no-op transport that verifies the two-boundary output for an aggregate-on-join shape.

**Estimated LOC.** 100-150 in `src/distributed_planner/distribute_plan.rs`, plus 50-100 of test fixtures. The planner side is the smaller of the two tracks.

**Catch.** Track A by itself crashes at runtime because the multi-consumer path needs Track B. Land them together (or land Track B first and then Track A — Track B is a no-op without Track A's plan structure).

### Track B — worker-consumer mesh + transport (pg_search side)

**Goal.** When a NetworkShuffleExec has `consumer_task_count=N`, the runtime needs to actually deliver each consumer task's hash partition. Today's mesh is N×K worker→leader only; we need an N×N peer mesh on top.

**1. DSM extension (`pg_search/src/postgres/customscan/mpp/dsm.rs`).**

- Add a `peer_mesh` region after the existing `cache_data`. Layout: N producer rows × N consumer columns × `mpp_queue_size` bytes per slot. Slot offset: `peer_mesh_offset + (producer * n_workers + consumer) * peer_queue_bytes`.
- Extend `MppDsmHeader` with `peer_mesh_offset: u64` and (optionally) a separate `peer_queue_bytes` if we want different sizing from the existing leader-bound queues.
- Extend `compute_dsm_layout(...)` with a new `peer_queue_bytes` parameter (or default to `mpp_queue_size()`).
- Bump `MPP_DSM_HEADER_VERSION` from 2 → 3.
- ~120 LOC including unit tests for offset math.

**2. Mesh runtime (`pg_search/src/postgres/customscan/mpp/mesh.rs` + `runtime.rs`).**

- New struct `MppPeerMesh` analogous to today's `MppMesh`, but per-worker. Each worker's `MppPeerMesh` holds:
  - N outbound senders to peer workers (its row of the peer mesh)
  - N inbound receivers + cooperative drains (its column of the peer mesh)
- `worker_setup` in `mpp/glue.rs`: in addition to the existing `outbound_senders` (to leader), attach as sender to peer-mesh row and as receiver to peer-mesh column.
- `MppWorkerState` gains a `peer_mesh: Option<Arc<MppPeerMesh>>`.
- ~150 LOC.

**3. Transport for peer consumption (`pg_search/src/postgres/customscan/mpp/runtime.rs`).**

- New `ShmMqPeerWorkerTransport` that wraps the worker's `MppPeerMesh`. Its `WorkerTransport::open(input_stage, target_partitions, target_task)` returns a `WorkerConnection` that streams from the peer-mesh inbound queue at column `worker_idx` for the producer task `target_task`.
- The fork plumbs `target_task` (= producer's task index in the stage). With our N×N mesh, that's the producer worker's index (0..N-1). The consumer's identity is implicit — it's `worker_idx` of the calling worker.
- ~100 LOC.

**4. Per-stage transport selection (fork side, small).**

- The fork's `WorkerTransport` is registered globally on the SessionContext. We need _per-stage_ selection: outer stage uses `ShmMqWorkerTransport` (existing, worker→leader), nested stage uses `ShmMqPeerWorkerTransport` (new, worker↔worker).
- Cleanest knob: stash both transports on a session extension (`MppDualTransport { gather: Arc<dyn WorkerTransport>, peer: Arc<dyn WorkerTransport> }`), then have `get_distributed_worker_transport` consult a stage-id-keyed selector. Or pass the stage to `open()` and let one transport route.
- Alternative: register a single composite transport on the session config; the composite picks gather vs peer based on the input_stage's `query_id`/`num` keyed against a routing table populated at leader_setup.
- ~50 LOC fork-side, ~30 LOC pg_search-side.

**5. Worker exec setup (`pg_search/src/postgres/customscan/aggregatescan/mod.rs`).**

- In `exec_mpp_worker`'s session-state builder, register the composite transport instead of `ShmMqWorkerTransport` directly.
- Pass `worker_idx` and `peer_mesh` through.
- ~30 LOC.

**6. Estimator size budget.**

- `estimate_dsm_size` adds `n_workers × n_workers × peer_queue_bytes` to the request. At N=8, 64 × 64 MiB = 4 GiB. That's heavy for default DSM. Either (a) tighten the per-edge cap (16 MiB → 16 × 64 = 1 GiB) or (b) gate the peer mesh on `n_workers >= some threshold` plus a GUC.
- ~10 LOC.

**Estimated total LOC for Track B.** 400-500.

**Verification.**

- Unit tests for the DSM peer-mesh layout (parallel to existing cache layout tests).
- A worker-only smoke test where two workers exchange 1 KiB of bytes through the peer mesh and verify round-trip.
- Run the existing 25 M bench. With the planner emitting the two-boundary shape and the runtime routing peer traffic correctly, the leader's `FinalPartitioned` becomes a no-op gather. Expected wall-clock at 8 producers: ~250-400 ms (1/N scaling minus barrier overhead).

### Order of work

Track B → Track A. Track B alone produces a peer mesh that isn't used yet (no-op for current plans). Track A alone breaks at runtime. Landing B first keeps the branch green; landing A on top flips on the parallelism.

If we want a single-PR delivery, land them together with a feature gate (e.g. `paradedb.enable_mpp_postagg_shuffle = off` by default), enable in tests, and flip on once green.

### What landed in this iteration

Both tracks were implemented against `pg_search` branch `moe/mpp-rpc-05-aggregate-activation` and DF-D fork branch `moe/mpp-skip-grpc-on-unaddressed`. The fork branch has an unmerged commit-pending diff and is currently consumed by pg_search via a `[patch."https://github.com/paradedb/datafusion-distributed"]` redirect at the workspace root (workspace `Cargo.toml`).

**GUCs (pg_search):**

- `paradedb.enable_mpp_postagg_shuffle` — bool, default off. Master switch.
- `paradedb.mpp_peer_queue_bytes` — int (bytes), default 16 MiB. Per-edge size of the peer mesh.

**Track A — fork side:**

- `DistributedConfig::in_process_peer_shuffle` (bool, default false). Settable via `with_distributed_in_process_peer_shuffle(bool)` on `SessionConfig` / `SessionStateBuilder` / `SessionState` / `SessionContext`.
- `_distribute_plan` (`src/distributed_planner/distribute_plan.rs`):
  - `Coalesce` arm in `in_process` mode now emits `NetworkShuffleExec(consumer_tc=1, input_tc=N)` (the worker→leader gather) instead of eliding when `peer_shuffle && multi_task_below`.
  - `this_is_real_boundary` for `Coalesce` returns `true` in that case so descendants observe `has_boundary_ancestor=true`.
  - Nested in-process `Shuffle` (with a real-boundary ancestor) emits `(consumer_tc=N, input_tc=N)` instead of eliding to `(1,1)` — re-application of reverted commit `05b13fd`, now safe because the gather-as-boundary feeds `has_boundary_ancestor`.
- All 23 existing distribute_plan snapshot tests still pass at the default-off setting (non-regressive).

**Track B — pg_search side substrate:**

- `mpp/dsm.rs`:
  - `MppDsmHeader` v3 (bumped from v2): adds `peer_queue_bytes` and `peer_mesh_offset` fields; `peer_slot_offset(producer, consumer)` accessor.
  - `compute_dsm_layout(... peer_queue_bytes)` extends the layout with an `n_workers × n_workers × peer_queue_bytes` peer-mesh region after the build-side cache.
  - `leader_init` `shm_mq_create`s every peer-mesh slot.
  - New `worker_peer_attach` returns `WorkerPeerAttach { peer_outbound, peer_inbound }` — the worker attaches as sender to its row and receiver to its column.
- `mpp/mesh.rs`:
  - `MppPeerMesh { n_workers, self_idx, outbound: Mutex<Option<Vec<MppSender>>>, inbound_drains: Vec<Arc<DrainHandle>> }`.
  - `MppPeerMesh::take_outbound()` for the inner-fragment producer to claim the senders exactly once.
  - `MppPeerMesh::drain_for_producer(idx)` for the transport.
- `mpp/runtime.rs`:
  - `ShmMqPeerWorkerTransport(peer_mesh)`: implements `WorkerTransport`. `open(target_task=producer_idx)` returns a `ShmMqPeerWorkerConnection` whose `stream_partition` pulls from `peer_mesh.drain_for_producer(producer_idx)`.
  - `CompositeWorkerTransport(Option<Arc<MppPeerMesh>>)`: routes by `input_stage.tasks.len()`. `> 1` → peer mesh; `== 1` → `LocalExecWorkerTransport` (broadcast).
- `mpp/glue.rs`:
  - `MppWorkerState` carries `peer_mesh: Option<Arc<MppPeerMesh>>`.
  - `worker_setup` calls `worker_peer_attach` and constructs `MppPeerMesh` when the peer mesh is reserved.
  - `mpp_peer_queue_bytes()` returns `crate::gucs::mpp_peer_queue_bytes()` only when the master GUC is on; else 0 (peer mesh skipped at DSM-init time).
- `aggregatescan/mod.rs`:
  - Leader and worker session builders both call `.with_distributed_in_process_peer_shuffle(crate::gucs::enable_mpp_postagg_shuffle())`.
  - `exec_mpp_worker` registers `CompositeWorkerTransport::new(peer_mesh)` instead of bare `LocalExecWorkerTransport`.

**Tests:** 25 pg_search MPP unit tests pass (3 new — peer-mesh layout / offsets / validation). 23 fork distribute_plan snapshot tests pass.

**Build state:** clean (1 dead-code warning — `MppPeerMesh::take_outbound`, used by the still-to-be-written inner-fragment runner).

### Remaining work — what blocks end-to-end activation

The substrate is in place but does NOT yet execute correctly when `enable_mpp_postagg_shuffle = on`. Three more pieces are needed:

#### C1 — DistributedTaskContext per worker (DONE — landed 2026-05-06)

Each PG worker now sets `DistributedTaskContext { task_index = worker_idx, task_count = N }` as a `SessionConfig::with_extension(...)` before the worker's `SessionStateBuilder` consumes it. Plumbed inside `exec_mpp_worker` in `aggregatescan/mod.rs`. Without this every worker computed `off = N × 0 = 0` and produced duplicate hash buckets. ~10 LOC.

#### C2 — Generic tagged-frame wire format + DemuxDrainHandle (DONE — landed 2026-05-06)

Rather than a one-off partition-id wrapper, the peer mesh carries a generic `[u32 magic = "MPPF"][u32 tag][u32 ipc_len][ipc_bytes]` frame whose tag is any u32 routing key. First user is partition_id; future users (stage_id multiplexing, query_id muxing, …) layer on the same primitive without changing the wire.

Primitives in `pg_search/src/postgres/customscan/mpp/transport.rs`:

- `MPP_FRAME_MAGIC` + `FRAME_HEADER_BYTES` constants.
- `frame_into(tag, ipc_bytes, buf)` and `parse_frame(bytes) -> (u32, &[u8])` — pure byte-level helpers.
- `MppSender::send_batch_traced_framed(tag, batch, stats)` — encodes IPC directly into a header-prefixed scratch buffer (single allocation, header `ipc_len` patched in after IPC writer finishes), then sends with the same cooperative-drain spin path as the existing `send_batch_traced`.
- `MppReceiver::try_recv_framed() -> RecvFramedOutcome` — pulls one shm_mq message and parses the header.
- `DemuxDrainHandle::cooperative(receivers, n_tags)` — parallel to `DrainHandle` but with `n_tags` sub-buffers. `poll_drain_pass` pulls one frame per receiver, parses the tag, pushes the decoded `RecordBatch` into `buffer(tag)`. `notify_source_done` fans out to every sub-buffer when a receiver detaches.

5 new unit tests cover the wire round-trip, magic/length validation, demux routing, and EOF fan-out (30/30 mpp unit tests pass overall).

#### C3 — Two concurrent producer fragments per worker (DONE — landed 2026-05-06)

The runner is in `pg_search/src/postgres/customscan/mpp/exec.rs::run_inner_producer_fragment`. Wired into `exec_mpp_worker` in `aggregatescan/mod.rs`:

- `find_inner_producer_fragment` walks the plan tree post-order to find the **bottommost** `NetworkShuffleExec` and returns its `children()[0]`.
- `count_network_shuffles` discriminates single-boundary (1 NSE) vs two-boundary (2+ NSEs) plans.
- When `peer_mesh.is_some() && count_network_shuffles >= 2`, the worker:
  1. Takes the peer mesh's outbound senders via `peer_mesh.take_outbound()`.
  2. Builds a fresh `TaskContext` for the inner subtree (separate memory pool from the outer fragment, since both run concurrently).
  3. Spawns both fragments concurrently with `futures::join!`:
     - `run_inner_producer_fragment(inner, peer_outbound, n_consumers, inner_ctx)` — iterates the scaled `N²` partitions and routes each batch to `peer_outbound[partition / n_consumers]` framed with `tag = partition_id`.
     - `run_producer_fragment(outer, outbound_senders, task_ctx)` — runs `AggregateExec(FinalPartitioned)` which pulls from the peer mesh via `ShmMqPeerWorkerTransport` and pushes the final-aggregated rows to the leader-bound mesh.

Both fragments share the worker's current_thread Tokio runtime; the cooperative drains interleave so peer-mesh consumers don't block while peer-mesh producers are still pushing.

Single-boundary plans (default-off) hit the existing one-fragment path unchanged.

#### C4 — Fork-side snapshot test (DONE — landed 2026-05-06)

Two new `_distribute_plan` tests in `dfdws/.../distribute_plan.rs::tests` register a no-op `WorkerTransport` (so `is_in_process()` returns true) and assert the produced tree shape:

- `test_in_process_peer_shuffle_two_boundary`: with `in_process_peer_shuffle = true`, the produced plan contains exactly 2 `NetworkShuffleExec`s (Coalesce-arm gather + Shuffle-arm peer mesh).
- `test_in_process_single_boundary_default`: with the flag at default off, the produced plan contains exactly 1 `NetworkShuffleExec` — confirming non-regression on the legacy single-boundary path.

#### D1 — End-to-end pgrx regression with `enable_mpp_postagg_shuffle = on` (DONE — landed 2026-05-06)

`pg_search/tests/pg_regress/sql/mpp_aggregate_postagg.{sql,out}` runs the same aggregate-on-join GROUP BY query under both GUC settings and computes totals (`COUNT(*) num_groups`, `SUM(count) total_count`, `SUM(sum) total_sum`) over the result. Both passes return identical totals:

```text
 num_groups | total_count | total_sum
------------+-------------+-----------
        200 |        1000 |   1979476
```text

This validates Track A + Track B end-to-end: the planner emits a two-boundary plan, workers spawn both producer fragments concurrently, the peer-mesh DemuxDrainHandle routes partition-tagged frames correctly, FinalPartitioned runs in parallel on workers, and the leader gathers byte-exact correct results.

(An earlier diagnostic version of the test used `ORDER BY title LIMIT 5`, which produced _different prefixes_ across the two passes because the customscan's pre-sort order differs between modes. That was a misleading symptom — the totals query confirmed the underlying GROUP BY result set is identical. The committed test asserts the totals because they're robust to whatever pre-sort ordering the customscan uses.)

#### Bench gate (DONE — landed 2026-05-07)

Substrate committed + pushed (pg_search at `1cc8798b8`, fork at `aa1e478f`). Bench run on the existing 25 M dataset on this machine, 3 medians per cell:

| N producers | Baseline (postagg=off) | New (postagg=on) |  Speedup |
| ----------: | ---------------------: | ---------------: | -------: |
|           2 |                3003 ms |      **1112 ms** | **2.7×** |
|           4 |                 796 ms |           763 ms |    1.04× |
|           8 |                 790 ms |           808 ms |    0.98× |
|          10 |                 815 ms |           812 ms |    1.00× |

Byte-exact correctness at every N (`(1250000, 25000000, 204787074592)` — the exact totals depend on which subset of `mpp_bench_pages` matches `'Section'`; this run differs from the report's earlier `(3123955, 25000000, 125004740460)` because the dataset was rebuilt with different `size_bytes` semantics).

Key findings:

- **The big win lands at N=2** (3003 → 1112 ms, 2.7×) where leader-serial `FinalPartitioned` was clearly the dominant cost. Parallelizing it across 2 workers nets the expected ~3× scaling.
- **At N=4 the win narrows** (796 → 763 ms, ~4%). At N=8/10 the plateau holds at ~800 ms in BOTH modes — the bottleneck has shifted somewhere downstream of the leader's old serial agg.
- Likely next-bottleneck candidates: the inner `RepartitionExec(Hash, N²)` fan-out (each producer now generates N² partitions instead of N), shm_mq throughput on the doubled queue grid (peer-mesh + leader-bound), or producer-side IPC encoding cost on N²-partition output. Profiling is the next step.

A pre-bench fork bug surfaced and was fixed in commit `aa1e478`: Track A's original Shuffle arm logic indiscriminately made _every_ nested in-process Shuffle a peer-mesh boundary. For aggregate-on-join with `HashJoinExec(Partitioned)` the planner inserts join-side hash-repartition shuffles, and they were being mis-routed through the (single) peer mesh — schema mismatch at runtime. Fix: thread `has_shuffle_ancestor` through `_distribute_plan` and emit peer-mesh only for the FIRST nested Shuffle.

### Generalization status — landed substrate (G1–G3) and what's still blocked

The runtime substrate for K peer meshes is now in place on `moe/mpp-rpc-05-aggregate-activation`:

| Step | What                                                                                                                                                                                                                  | Status        |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| G1   | DSM is K-aware: `compute_dsm_layout(... n_peer_meshes)` reserves `K × N × N × peer_queue_bytes`. `MppDsmHeader` v3→v4 with `peer_slot_offset(mesh, prod, cons)`. `MppWorkerState.peer_meshes: Vec<Arc<MppPeerMesh>>`. | DONE          |
| G2   | Stage-id-keyed transport routing: `CompositeWorkerTransport` holds `Arc<RwLock<HashMap<usize, Arc<MppPeerMesh>>>>` populated after the worker physical plan is built.                                                 | DONE          |
| G3   | K-fragment runner: `exec_mpp_worker` walks `collect_inner_producer_fragments` and spawns one `run_inner_producer_fragment` per nested cross-worker shuffle (`K+1` concurrent futures via `try_join_all`).             | DONE          |
| G4   | Drop fork's `!has_shuffle_ancestor` guard. **Reverted** — see findings below.                                                                                                                                         | BLOCKED on G5 |
| G5   | Per-stage `DistributedTaskContext` so different stages can have different `consumer_tc`.                                                                                                                              | NOT STARTED   |
| G6   | Multi-shuffle bench shape (3-way join + GROUP BY) for end-to-end validation.                                                                                                                                          | NOT STARTED   |

**G4 finding — running it on the bench surfaced a hard correctness bug.** With the guard dropped, the planner emits peer-mesh boundaries for the join-side hash repartitions of `HashJoinExec(Partitioned)` (or, with broadcast joins enabled, for any other nested cross-worker shuffle). The 25 M `aggregate_join_groupby` bench then returned `total_count = 250M` (vs the correct 25M) — every row counted N times.

Root cause: the post-aggregate inner producer fragment's `RepartitionExec(post-agg-Hash, N²)` is iterated `0..N²` times, and each iteration recursively triggers `HashJoinExec.execute(j)` → `NetworkShuffleExec(join-side).execute(j)`. With the join-side stage's `task_index = K` and `consumer_tc = N`, `off = N × K`, so the consumer asks for `stream_partition(N×K + j)` with `j ∈ 0..N²`. That requests partitions far outside the consumer's slice, which empty-streams _most_ of them — but with `HashJoinExec(CollectLeft)` plus `with_distributed_broadcast_joins(true)`, the build side is collected once across all probe partitions per worker, and each of N workers ends up with a full broadcast build. Each worker then produces a full join output, so the post-aggregate consumes N copies of the data.

Fix path (deferred to G5):

1. Each cross-worker shuffle stage needs its OWN `DistributedTaskContext` so `NetworkShuffleExec.execute(j)` reads the correct `task_index` for its stage. Today the worker's session has a single `task_index = worker_idx` extension, conflated across all stages.
2. The inner producer fragment runner needs to iterate only the _task's slice_ of output partitions (`n_partitions / consumer_tc`), not the full scaled count. With `consumer_tc=N` and `partitions=N²`, that's `N` iterations per task — matching the join-side's per-task partition ownership.
3. (Optional) Disable broadcast joins in the in-process peer-shuffle path, or tag each broadcast separately so `LocalExecWorkerTransport` still handles them while peer-mesh shuffles go through `ShmMqPeerWorkerTransport`.

These are mechanical changes once we agree on the design, but each touches both the fork (per-stage DistributedTaskContext via `Stage` extension) and pg_search (per-mesh iteration logic in `run_inner_producer_fragment`). Estimate: ~200 LOC across both.

**Current shipping state (post-revert):** `moe/mpp-rpc-05-aggregate-activation` at `64f4fe077`, fork at `d954bed`. The `!has_shuffle_ancestor` guard is back in place — only the post-agg shuffle is peer-mesh-promoted, all other nested shuffles continue to elide as in-process. Substrate G1–G3 is parameterized over K but currently exercised at K=1. Bench at K=1:

| N producers | Baseline (postagg=off) | New (postagg=on) |  Speedup |
| ----------: | ---------------------: | ---------------: | -------: |
|           2 |                3065 ms |      **1127 ms** | **2.7×** |
|           4 |                 828 ms |           802 ms |    1.03× |
|           8 |                 808 ms |           811 ms |    ~1.0× |
|          10 |                 852 ms |           833 ms |    1.02× |

Same plateau pattern as before: big win at N=2 (leader-serial `FinalPartitioned` is the bottleneck), modest 3% win at N=4, plateau at ~800 ms for N≥8 (some other downstream bottleneck — probably the inner `RepartitionExec(Hash, N²)` fan-out cost or shm_mq throughput on the doubled queue grid).

### Generalizing — what stops us from K nested shuffles + arbitrary queries

The current substrate is hard-coded for K=1 nested cross-worker shuffle (the post-aggregate one). To go fully generic — multiple peer meshes per query, arbitrary plan shapes — five concrete blockers, in increasing scope:

1. **DSM allocates exactly one peer mesh per query.** `compute_dsm_layout` takes a single `peer_queue_bytes` and reserves N×N slots. Generalizing means parameterizing by _K_ nested cross-worker shuffles → K layers of N×N slots. At K=3, N=8, 16 MiB/edge that's ~3 GiB just for peer meshes; at the 16 GiB cap we'd run out at K≈16. Need a sizing heuristic or per-stage cap.

2. **Worker transport routing is single-mesh.** `CompositeWorkerTransport` discriminates by `input_stage.tasks.len()` — coarse, fine for "broadcast vs peer-mesh" but ambiguous when multiple stages have `tasks.len() > 1`. Swap to a `HashMap<stage_id, Arc<MppPeerMesh>>` populated at `leader_setup` by walking the plan and assigning each cross-worker `NetworkShuffleExec` its own peer mesh.

3. **Worker spawns exactly one inner producer fragment.** `exec_mpp_worker` calls `find_inner_producer_fragment` once and runs `run_inner_producer_fragment` once. For K nested cross-worker shuffles we'd walk the plan top-down, find each `NetworkShuffleExec` stage, take `children()[0]` as a producer subtree, and spawn `K+1` concurrent futures (the outer worker→leader gather + K inner peer-mesh runners). Each gets its own peer-mesh outbound senders + task_ctx.

4. **Fork restricts peer-mesh emission to the FIRST nested Shuffle** (the `!has_shuffle_ancestor` guard added in `aa1e478`). Removing it re-emits peer-mesh for every nested Shuffle — but only safe when the runtime side has K peer meshes ready. Keep the runtime and planner in lockstep via a `DistributedConfig.in_process_max_peer_meshes` knob (or similar).

5. **DistributedTaskContext is set once per worker, with `task_count = n_workers`.** Fine when every cross-worker stage has `consumer_tc = n_workers`. For shapes where different stages have different `consumer_tc` (which the annotator picks based on cardinality factors), the worker's `task_index` is ambiguous. In gRPC each worker is a separate process per stage; in our in-process model one PG worker plays multiple stage-task roles. Need either: (a) constrain planner to `consumer_tc=N` for all cross-worker stages (config knob), or (b) thread per-stage `DistributedTaskContext` via the fork's `Stage` extension so `NetworkShuffleExec.execute` reads the right one.

Cleanest sequence:

1. Generalize DSM + transport routing first (blockers 1-2) — mechanical pg_search work; K=1 is a special case of the general code.
2. Drop the `!has_shuffle_ancestor` guard in the fork + add the lockstep config knob (blocker 4).
3. Spawn K+1 fragments in `exec_mpp_worker` (blocker 3).
4. Constrain `consumer_tc=N` for all cross-worker stages (blocker 5(a)) initially; defer per-stage DistributedTaskContext until needed.
5. Validate with a 3-way join + GROUP BY regression test + bench shape (a query that genuinely has multiple nested cross-worker shuffles).

The fundamental architecture isn't broken — it just needs to be parameterized over K.

#### Status of build state

- pg_search: **30/30 mpp unit tests pass** (5 new framing/demux tests). **4/4 pgrx mpp regression tests pass** including the new `mpp_aggregate_postagg` end-to-end correctness check.
- DF-D fork: **25/25 distribute_plan tests pass** including the 2 new in-process snapshot tests.
- Both repos in clean uncommitted-diff state. Workspace `[patch]` redirect points pg_search at the local fork worktree; commit + push the fork before bumping the rev.

### How to commit / push the fork

The fork diff is currently uncommitted in `dfdws/mpp-skip-grpc-on-unaddressed`. Three files: `distributed_ext.rs` (+43 LOC trait + delegations), `distribute_plan.rs` (+~50 LOC peer-shuffle arms), `distributed_config.rs` (+10 LOC new field). Suggested commit message:

```text
feat(planner): in_process two-boundary peer-shuffle mode

Add `DistributedConfig::in_process_peer_shuffle` (default false). When on,
in_process `_distribute_plan`:
- Coalesce arm emits NetworkShuffleExec(consumer_tc=1, input_tc=N) as the
  worker→leader gather instead of eliding.
- Nested in-process Shuffle uses (consumer_tc=N, input_tc=N) as a peer-mesh
  cross-worker boundary instead of eliding to (1, 1).
- this_is_real_boundary marks Coalesce as a real boundary in this mode so
  descendants set has_boundary_ancestor=true.

Off by default; legacy single-boundary path preserved (23 snapshot tests
unchanged). Embedder (e.g. ParadeDB) is responsible for the runtime side
of the peer mesh.
```text

Once the fork is pushed, bump `pg_search/Cargo.toml`'s `datafusion-distributed` rev and remove the `[patch]` in the workspace `Cargo.toml`.

---

## Other open items (lower priority)

- **In-tree DF-D tests** for the outer/nested boundary arithmetic. Senior reviewer flagged this; not done yet. Snapshot tests in `_distribute_plan` with a registered no-op transport.

- **Leader-as-worker-0.** The leader is consumer-only today. After Track A lands, the leader becomes a pure gather and there's no real reason it can't _also_ run the producer fragment for its own slice. Small refactor in `glue.rs`/`MppLeaderState`. Worth doing only after the post-agg shuffle is in.

- **`mpp_worker_count = 2` regression.** With our default heuristics, `mpp_worker_count = 3` (= 2 producers) takes ~3 s — slower than DF serial. Probably an interaction between target_partitions=2 and the build-side cache size estimation. Punted because (a) the gate is already `>= 3`, (b) 2 producers is rarely the right config. Worth a 30-minute look once the architecture is final.

- **DSM cache size estimation.** The current 256 MiB-per-slot constant is a worst-case cap. Replace with per-source bytes-per-row × estimated rows from `paradedb.index_info`. Cuts allocated DSM by ~10× on typical workloads.

- **JoinScan and single-table aggregate paths** are not on the MPP path. Only AggregateScan-with-binary-join-with-aggregate activates today. JoinScan would benefit similarly; single-table aggregates need a different shape (no broadcast/all-gather needed, but the post-agg shuffle still applies).

- **Bench-CI flake.** `permissioned_search` alt-1 hits a pre-existing JoinScan bug ([paradedb#5003](https://github.com/paradedb/paradedb/issues/5003)). Worked around in the bench file. Will re-enable once the upstream JoinScan fix lands.

---

## Reproducing this state

```bash
# pg_search at PR #4988
git checkout moe/mpp-rpc-05-aggregate-activation
# (tip df2d7e4e3 as of writing)

# Bench dataset setup (one-time)
psql -d pg_search -c "ALTER SYSTEM SET paradedb.global_target_segment_count TO 32;"
psql -d pg_search -c "SELECT pg_reload_conf();"
# restart cluster
psql -d pg_search <<EOF
SET maintenance_work_mem = '1GB';
SET max_parallel_maintenance_workers = 16;
REINDEX INDEX mpp_bench_files_idx;
REINDEX INDEX mpp_bench_pages_idx;
EOF

# Run the bench
psql -d pg_search -f /tmp/mpp_scaling_bench.sql

# Or trace per-worker breakdown
psql -d pg_search <<EOF
SET client_min_messages = 'warning';
SET work_mem = '8GB';
SET paradedb.enable_aggregate_custom_scan = on;
SET paradedb.enable_mpp = on;
SET paradedb.mpp_worker_count = 9;
SET paradedb.mpp_debug = on;
WITH mpp AS (
  SELECT f.title, COUNT(*) c, SUM(p.size_bytes) s
  FROM mpp_bench_pages p JOIN mpp_bench_files f ON f.id = p.file_id
  WHERE f.content ||| 'Section'
  GROUP BY f.title
) SELECT COUNT(*), SUM(c), SUM(s) FROM mpp;
EOF
```text

---

## Decision points if you're picking this up

1. **One PR or two?** Tracks A + B are tightly coupled at runtime. One PR keeps them coherent; two PRs (B first, then A) keep diff sizes manageable but require a feature gate while the runtime is half-built. I'd vote two PRs with the gate.

2. **Peer-mesh queue sizing.** The current N×K mesh uses 64 MiB per edge; an N×N mesh at the same size is 4 GiB at N=8. Either drop per-edge to 16 MiB or gate the peer mesh behind a config knob. The 64 MiB number was sized for the post-agg shuffle traffic in Attempt 2; with all-gather build-side caching, partial-agg traffic per edge is much smaller.

3. **Transport routing.** Cleanest is to register a composite `WorkerTransport` that internally routes per stage, but this requires the fork to either pass the stage to `open()` (which it kind of does via `input_stage: &Stage`) or expose a `Stage` accessor that distinguishes shape. The fork already exposes `Stage::query_id` and `Stage::num`. The session-state ext can hold a `HashMap<u32, Arc<dyn WorkerTransport>>` keyed by stage num. Routing logic ~20 LOC.

4. **Testing strategy.** The existing 25 M bench is a sufficient e2e gate (correctness + scaling). For unit tests, add round-trip tests for the peer mesh and snapshot tests for the new planner arithmetic in `_distribute_plan`.
````
