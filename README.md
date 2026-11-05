# Limit Order Book & Matching Engine

A multithreaded price-time-priority matching engine in C++17, with a binary TCP order gateway and a binary market-data feed. It uses lock-free ring buffers between threads, and a pool-backed book that does not allocate while matching. Trades reach Kafka, and order state reaches Redis, without either one being on the matching path.

The order flow is synthetic and the engine handles one instrument. This is a systems project, not a trading venue.

```
             orders                                       binary feed
clients ───TCP───> gateway ─ring─> matcher ─ring─> publisher ───TCP───> feed_client
        <──TCP──── thread          thread          thread   ───TCP───> kafka_bridge.py ──> Kafka
         acks, fills                (the book)        │
                                                      └─ pipelined ──> Redis (order state)
```

The gateway owns socket reads, the matcher alone touches the book, and the publisher owns every socket write. Each ring has exactly one producer and one consumer. A full ring back-pressures the stage behind it, so no order is dropped between stages. The publisher never waits on a socket: it buffers up to a limit per peer and then cuts that peer off, so a slow client or a dead Redis cannot stall matching.

## Quick start

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure

./build/engine --port 9000 --feed-port 9001 &
./build/loadgen --port 9000 --orders 1000000 --batch 64 --window 8192
./build/bench
```

The full stack, with Redis, Redpanda (a Kafka-compatible broker), the engine, and the bridge:

```bash
docker compose up -d --build
```

## Design notes

| Choice | Why |
|---|---|
| Direct-indexed price ladder | Finding a level is an array access. It trades memory for speed, and limits prices to a configured tick range. |
| Bitmap over the ladder | After a level empties, the next occupied price is found one 64-tick word at a time with `ctz`/`clz`, not tick by tick. |
| Intrusive FIFO per level, nodes from a pool | Adding at the tail is O(1), and nothing allocates after startup. |
| Open-addressing id map | Cancel is O(1) wherever the order sits in its queue. |
| SPSC rings with cached indices | Each side only touches the other's cache line when its cached value says full or empty. |
| Fixed-size binary frames | 18-byte order messages and 48-byte feed frames, little endian, with no parsing or allocation per message. |

**Wire formats.** Orders and replies: `type u8 | id u64 | price u32 | qty u32 | flags u8`. Every message gets exactly one `Ack`, `CancelAck`, or `Reject`, after any `Trade` frames. The feed: see [`include/lob/feed.hpp`](include/lob/feed.hpp). Each frame carries a sequence number, and `FeedHandler` rebuilds the book from frames alone and detects gaps.

## Performance

Measured on a shared 8 GB, 8-core MacBook (4 performance + 4 efficiency cores) with other applications open, in a Release build. macOS has no core pinning. Read these as this machine on a busy day: throughput is steady between runs, but tail latency is not. Everything is reproducible with `benchmarks/run.sh`, and the raw results are in [`benchmarks/results/`](benchmarks/results/).

**In-process** (no sockets), 5,000,000 events of synthetic flow:

| Measure | Result |
|---|---|
| Book alone, one thread | 19.4M events/sec (median of 5 runs) |
| One book operation | p50 42 ns, p95 208 ns, p99 292 ns |
| Through both rings, three threads, flat out | 14.9M events/sec |
| Through both rings, paced at 500K events/sec | p50 under 1 µs |

At that paced rate, p99 depends on what else the machine is doing. It was 5 µs in the quietest run and 36 ms in the run recorded in `inprocess.json`. Steady microsecond tails would need pinned cores on an isolated machine, which this was not.

**Through TCP on loopback**, round trip from when a message was due until its reply arrived, including the load generator's own send cost:

| Offered load | Achieved | p50 | p95 | p99 |
|---|---|---|---|---|
| 5,000 msgs/sec | 4,999 | 356 µs | 419 µs | 486 µs |
| 50,000 msgs/sec | 49,997 | 282 µs | 491 µs | 1.09 ms |
| 200,000 msgs/sec | 199,990 | 281 µs | 448 µs | 623 µs |
| 400,000 msgs/sec | 399,978 | 275 µs | 405 µs | 547 µs |
| Flat out, one connection | 537K–655K | 12.5 ms | 16.5 ms | 18.5 ms |

These are medians of 3 runs. Flat-out latency is queueing: at capacity, messages wait behind the 8,192 allowed in flight.

**In Docker**, with Redis and the Kafka bridge attached, at 5,000 msgs/sec on the compose network: p50 298 µs, p95 714 µs, p99 2.2 ms, no failures. Docker Desktop's Linux VM adds overhead that a Linux host would not have.

### Two things the measurements taught me

**One syscall per message is the real bottleneck at moderate load.** The first open-loop runs showed p95 above one second at 50K msgs/sec, while 400K looked better. At 50K, usually only one message is due at a time, so the sender makes one `send()` per message, cannot keep up, and falls behind. At 400K, every call carries a batch. Holding a short batch for up to 200 µs fixed it, even with that wait counted as latency:

| 50,000 msgs/sec, median of 3 | p50 | p95 | p99 |
|---|---|---|---|
| One syscall per message | 150 ms | 1.76 s | 1.99 s |
| Coalesce up to 200 µs | 297 µs | 480 µs | 988 µs |

**Keeping Redis off the matching path matters most in the tail.** `--sync-redis` makes the matcher wait for Redis to acknowledge each book change, as a baseline. Both modes ran the same flow at 1,000 msgs/sec, against Redis in Docker Desktop, and ended with identical state: 19,286 commands and 2,448 keys, none dropped.

| 1,000 msgs/sec, median of 3 | p50 | p95 | p99 |
|---|---|---|---|
| Blocking write-through | 695 µs | 327 ms | 469 ms |
| Async pipelined publisher | 181 µs | 1.1 ms | 4.9 ms |

The p50 drop of about 74% is steady across runs. The p95 is noisy: it ranged from 2 ms to 1 s for blocking and from 0.3 ms to 19 ms for async. The blocking baseline is deliberately naive and sits close to its limit at this rate, so this shows the value of the design rather than a production gain.

**Kafka.** At 20,000 msgs/sec the bridge delivered all 47,873 trades to the topic with 0 sequence gaps. The Python bridge is slower than the feed and catches up afterwards, which is the point of keeping it off the matching path. It does not keep up indefinitely at high rates, and past its buffer limit the engine drops it.

## How it is tested

```bash
ctest --test-dir build --output-on-failure      # 41 GoogleTest cases
python/.venv/bin/python -m pytest python/tests  # bridge, including against the real engine binary
```

- **Differential testing.** 600,000 random operations run through the fast book and a deliberately plain reference book (ordered maps of deques), which must agree on every trade, in order.
- **End to end.** A subscriber rebuilds the book from the binary feed alone, through TCP and all three threads, and must match the reference book on every price level.
- **Sanitizers.** The suite is clean under ThreadSanitizer, which is what actually checks the rings' memory ordering, and AddressSanitizer: `cmake -S . -B build-tsan -DLOB_SANITIZE=thread`.
- **Failure paths.** Clients that never read, garbage frames, frames split across writes, Redis being down, and ports in use.

## Known limits

- **One instrument.** There is one book in one process, so it does not scale horizontally. Multiple symbols would mean one matcher per symbol or shard.
- **No durability.** A restart loses the book. There is no journal or replay.
- **No feed recovery.** There is no snapshot message, so a subscriber must connect before the flow it wants to replicate. It can detect a gap but cannot repair one.
- **Trusted clients.** There is no authentication, no cancel-on-disconnect, and no check that a client owns the order it cancels. A real venue needs all three.
- **Bounded prices.** The ladder covers a configured tick range, and its memory grows with that range.
- **Kubernetes.** `k8s/deployment.yaml` passes schema validation (kubeconform), but it has not been applied to a live cluster.

## Layout

| Path | What |
|---|---|
| `include/lob/order_book.hpp` | The book |
| `include/lob/ring_buffer.hpp` | SPSC ring |
| `include/lob/memory_pool.hpp`, `flat_id_map.hpp` | Allocation-free storage |
| `include/lob/feed.hpp` | Feed format and `FeedHandler` replica |
| `src/server.cpp` | Gateway, matcher, and publisher threads |
| `src/bench.cpp`, `tools/loadgen.cpp` | In-process benchmark and open-loop TCP load generator |
| `tools/feed_client.cpp` | Feed subscriber with top-of-book display |
| `python/kafka_bridge.py` | Feed to Kafka |
| `benchmarks/run.sh` | Reproduces the numbers above |
