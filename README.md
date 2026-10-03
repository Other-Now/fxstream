# fxstream

An FX price aggregation and distribution engine in Java 21. It runs the core loop of an FX ECN or bank
pricing desk, scaled down:

1. **Intake.** Liquidity providers (LPs) stream two-way quotes over **FIX 4.4** via QuickFIX/J.
2. **Aggregation.** One thread per currency pair builds a composite best bid/offer from all providers
   and drops any provider whose feed goes stale. It is fed by a lock-free multi-producer ring buffer,
   and the tick path **allocates nothing**.
3. **Pricing.** Spreads are applied per client tier and trade size.
4. **Distribution.** Prices stream to clients over **RabbitMQ** topic exchanges. Slow consumers get
   **conflation**.
5. **Quotes and trades.** Clients can take firm **quotes with a time-to-live** and trade on them over
   AMQP. Trades are checked for expiry and price movement and stored in **PostgreSQL via JPA/Hibernate**
   exactly once, even across broker `kill -9`s. Quote and price audit history goes to **MongoDB**.

```
LP1..LPn --FIX 35=S--> FixPriceIntake --offer--> TickRing (MPSC) --> PairEngine (Book + Pricer)   x4 pairs
                                                                         |
                                       fx.prices [topic] price.<PAIR>.<tier> <--+--> MongoDB price_snapshots
POST /api/quotes --> QuoteService (memory + MongoDB quotes)
client --> fx.trades --> fx.trade.requests (quorum, DLX->DLQ) --> TradeListener --> TradeService --> Postgres (JPA)
                                                                        \--> fx.trade.results (confirmed) --> client
```

Stack: Java 21, Spring Boot 3.5, QuickFIX/J 3.0, RabbitMQ (AMQP 0-9-1, quorum queues), Spring Data
JPA/Hibernate + Flyway on PostgreSQL 16, Spring Data MongoDB, JUnit 5, JMH, HdrHistogram, GitHub Actions.

## Results

All numbers come from `scripts/bench-all.sh`. CI runs the same script on Linux and publishes the
results in the job summary. The local run was on an 8-thread Windows laptop with RabbitMQ in WSL2. That
setup adds about 0.5 ms per broker hop, so latency is shown for both environments.
Raw results: [`results/local/`](results/local/SUMMARY.md), [`results/ci/`](results/ci/SUMMARY.md).

| What | Local (8-thread Windows, broker in WSL2) | CI (GitHub Actions, 4 vCPU Linux) |
|---|---|---|
| Tick path: ring → book → 3 tiers priced (JMH) | **77–82 ns/tick**, ≈0 B/op | **52–60 ns/tick**, ≈0 B/op |
| Ring round trip / book recompute (JMH) | 11.8 ns / 27–35 ns | 8.1–8.4 ns / 22–38 ns |
| Core soak, no I/O, 30 s | 20.9M ticks/s, **0 GCs** | 6.0M ticks/s (4 CPUs), **0 GCs** |
| LP tick (FIX) → client price (RabbitMQ), 1k ticks/s | p50 **0.82 ms**, p99 1.9 ms | p50 **0.51 ms**, p99 11.8 ms |
| Bare broker hop, 1k msgs/s (baseline) | p50 0.50 ms, p99 0.93 ms | p50 0.29 ms, p99 329 ms (noisy runner) |
| Stale LP excluded (200 ms threshold), 20 trials | 202.4–203.4 ms | 200.7–202.9 ms |
| Slow consumer, conflated queue | depth ≤ **1**, prices 1.6 ms old, ends on latest | depth ≤ **1**, prices 1.3 ms old, ends on latest |
| Same consumer, plain queue | backlog 13,783, prices **10.3 s** old | backlog 14,809, prices **10.1 s** old |
| 1,000 quotes (200 ms TTL) traded 0–400 ms later | 610/610 late rejected, 0 wrong | 534/534 late rejected, 0 wrong |
| 15,000 trades through broker `kill -9`s | 5 kills: **0 lost, 0 duplicated** | 6 kills: **0 lost, 0 duplicated** |
| Same, idempotency off (control) | 57 dup rows, 36 double fills, 20 conflicting | 16 dup rows, 10 conflicting outcomes |

CI run with all results in the job summary: [37134138991](https://github.com/Other-Now/fxstream/actions/runs/37134138991).

Notes on the numbers:

- **Latency is measured from each tick's *scheduled* send time**, so stalls in the sender or the
  system count as latency (coordinated omission). The harness reports its own sender lag next to the
  result.
- **Tail latency on both machines is set by the box, not the engine.** Everything shares one host:
  4 vCPUs in CI and 8 threads locally, running the LPs, the service, the broker, Postgres and MongoDB.
  The bare-broker baseline shows the same tails. The p50s and the JMH/soak numbers are the stable ones.
- **Above about 5k ticks/s, local latency is set by the broker hop.** Each book change is 3 tier
  messages, so 10k ticks/s is about 15k msgs/s. The bare-broker baseline in WSL2 hits the same wall at
  that rate (p99 40 ms at 15k msgs/s).
- **"0 B/op" covers the pricing core only.** QuickFIX/J parsing and the AMQP client still allocate.
  The proof is a `ThreadMXBean` test (0 bytes over 1M ticks), JMH `-prof gc`, and the soak's GC count.

### Two things the measurements caught

1. **Idle parking on Windows.** The first engine version parked for 50 µs when idle. Latency at
   1k ticks/s came out *worse* than at 10k ticks/s (p50 16.7 ms vs 1.6 ms). On Windows,
   `parkNanos(50µs)` actually sleeps **15.8 ms**. The fix was the Disruptor-style blocking wait: spin,
   then yield, then park, with the producer calling `unpark()` on a sleeping engine. A Dekker-style
   handshake closes the lost-wakeup race. Measured wake-up is 62 µs (p50), and p50 at 1k ticks/s
   dropped to 0.82 ms.
2. **Retries without idempotency don't just duplicate.** In the control run a broker restart caused
   redelivery 40 s later. By then the price had moved, so the same order was stored as **FILLED** and
   then **REJECTED_PRICE_MOVED**, and the client received both answers.

## Design in brief

- **Single writer per pair.** The book is plain arrays mutated by one thread. All concurrency lives in
  the ring:
  - producers claim a slot by CAS on `tail` and publish with a per-slot release store;
  - the consumer reads with acquire;
  - slots are 64-byte cache lines and `head`/`tail` are padded apart.
- **Fixed-point prices.** Prices are `long`s in 1e-5 units. Markups are set in tenths of a pip, so JPY
  pairs work the same way.
- **Batching is conflation.** The engine drains everything queued and publishes once. The latency stamp
  comes from the *oldest* tick in the batch.
- **Prices and trades get opposite delivery guarantees:**
  - *Prices:* transient, no confirms, dropped while the broker is down.
  - *Trades:* persistent, publisher confirms, manual acks, a quorum queue, a DLQ and a delivery limit.
    The order is persist → publish result and wait for the confirm → ack.
- **Exactly-once effect.** Delivery is at-least-once. The consumer is made idempotent by a UNIQUE
  constraint on the order id, and a replay returns the stored answer.
- **Data access:**
  - JPA uses a pooled `SEQUENCE` (not IDENTITY), Flyway owns the schema, and `open-in-view=false`.
  - MongoDB stores quotes and per-second book snapshots, with a TTL index and a compound index.

The full walkthrough and likely interview questions are in [`docs/NOTES.md`](docs/NOTES.md).

## Running it

You need Java 21, Maven, PostgreSQL (database/user/password `fxstream`), MongoDB and RabbitMQ 3.12+ on
localhost.

```bash
mvn verify                                   # 13 core tests + 6 full-stack tests (needs the 3 services)
scripts/app.sh start                         # service on :8090, FIX acceptor on :9878
java -jar bench/target/fxstream-bench.jar latency --rates 1000
curl -s localhost:8090/api/prices
curl -s -XPOST localhost:8090/api/quotes -H 'Content-Type: application/json' \
     -d '{"clientId":"acme","pair":"EURUSD","side":"BUY","qty":1000000}'
RESTART_CMD="..." scripts/bench-all.sh       # everything; RESTART_CMD must kill -9 + restart the broker
```

## Scope (what this is not)

- The LPs are simulated, and everything runs on one machine with a single RabbitMQ node. There is no
  auth or TLS.
- Last look is symmetric (reject if the price moved either way beyond tolerance). Real FX last look is
  usually asymmetric.
- The trade store is PostgreSQL. Oracle was not used.
