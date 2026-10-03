# fxstream — interview study sheet

Read this top to bottom once, then read the code in the order of section 3. Everything here is
something an interviewer for an FX / low-latency Java role can reasonably ask about.

## 1. The 60-second pitch

> "fxstream is a small FX price aggregation and distribution engine in Java 21 — the core of what an
> FX ECN or bank pricing desk runs. Simulated liquidity providers stream two-way quotes over FIX 4.4.
> Each currency pair has one pricing thread fed by a lock-free multi-producer ring buffer; it keeps a
> composite best bid/offer across providers, drops providers whose quotes go stale, applies spreads per
> client tier and trade size, and streams prices to clients over RabbitMQ topic exchanges, with
> conflation for slow consumers. Clients can request a firm quote with a time-to-live and trade on it;
> trades are validated against expiry and price movement, stored in Postgres through JPA exactly once
> even when the broker is kill -9'd, and quotes and book snapshots are audited in MongoDB.
> The tick path allocates zero bytes, which I proved three ways, and every number in the README comes
> from a test that CI runs."

## 2. Architecture

```
 LP1..LPn (FIX 4.4 initiators, simulated)
      | 35=S Quote: 55 Symbol, 132/133 BidPx/OfferPx, 134/135 sizes, 5050 send-time (custom)
      v
 FixPriceIntake  (QuickFIX/J ThreadedSocketAcceptor: one thread per LP session)
      | engine.offer(lp, bid, ask, ...)        <- many producers
      v
 TickRing (MPSC, preallocated long[], 64 B/slot)   x 4 pairs
      | drain (single consumer)
      v
 PairEngine thread: Book (best bid/offer over fresh LPs) -> Pricer (tier x size markups)
      | onPrice(pair, tier, seq, bid, ask, freshLps, origin)
      v
 RabbitPricePublisher -> exchange fx.prices [topic], key price.<PAIR>.<tier>, 48-byte binary, transient
      |                     client queues: plain, or x-max-length=1 + drop-head (conflated)
      +-> SnapshotWriter -> MongoDB price_snapshots (1/s per pair, TTL 7d)

 REST POST /api/quotes -> QuoteService (in-memory + MongoDB quotes) -> quoteId, price, expiresAt
 client -> fx.trades [direct] -> fx.trade.requests (quorum, DLX -> fx.trade.requests.dlq)
        -> TradeListener (manual ack) -> TradeService (QuoteCheck, JPA insert w/ UNIQUE dedupe_key)
        -> fx.trade.results [topic] result.<clientId> (publisher confirm) -> ack request
```

## 3. Read the code in this order (≈ 1,200 lines that matter)

| # | File | What to take away |
|---|---|---|
| 1 | `core/.../Px.java` | prices are `long` 1e-5 units; no doubles |
| 2 | `core/.../TickRing.java` | the MPSC protocol (section 4.2) — the most likely deep-dive |
| 3 | `core/.../Book.java` | composite over N LPs, staleness, crossed book |
| 4 | `core/.../Pricer.java` | markups in tenths of a pip; JPY pip is 100x |
| 5 | `core/.../PairEngine.java` | batch = conflation; idle strategy + unpark; what gets published |
| 6 | `core/.../QuoteCheck.java` | expiry / price-moved decision as a pure function |
| 7 | `core/src/test/.../PairEngineTest.java` | the 0-byte allocation test and the wake-up test |
| 8 | `app/.../fix/FixPriceIntake.java` | QuickFIX/J acceptor, settings, where parsing allocates |
| 9 | `app/.../prices/RabbitPricePublisher.java` | channel per thread, reused buffer, transient prices |
| 10 | `app/.../trades/AmqpConfig.java` | quorum queue, DLX, delivery limit, manual-ack container |
| 11 | `app/.../trades/TradeListener.java` | ordering: persist -> publish+confirm -> ack |
| 12 | `app/.../trades/TradeService.java` | idempotency: lookup fast path + UNIQUE constraint |
| 13 | `app/.../trades/Trade.java` + `V1__trades.sql` | JPA entity, pooled sequence, Flyway |
| 14 | `app/.../audit/*` | MongoDB documents, TTL and compound indexes |
| 15 | `bench/.../TradeBench.java` | how "0 lost, 0 duplicated" is actually checked |

## 4. Concepts and the questions they come with

### 4.1 Why one thread per currency pair (single writer)?
The book is mutated on every tick. With one writer per pair there are no locks and no shared mutable
state in the hot path: the book is plain arrays. Concurrency is pushed into one place, the ring, where
it is solved once. Pairs are independent, so they scale across cores. This is the LMAX / "mechanical
sympathy" design. *Trade-off:* a single hot pair is limited to one core (~13M ticks/s in JMH terms —
far beyond any real LP feed).

### 4.2 The ring buffer (TickRing) — be ready to draw it
- Capacity is a power of two; index = `seq & mask`.
- Each slot is 8 longs = 64 bytes = one cache line: `seq, lp, bid, ask, bidQty, askQty, origin, pad`.
- **Producer:** CAS `tail` from t to t+1 (claims slot t), but only if `t - head < capacity`. Write the
  payload with plain stores, then `setRelease(slot.seq, t+1)`.
- **Consumer:** at head h, `getAcquire(slot.seq) == h+1` means published. Read, then `setRelease(head, h+1)`.
- *Why per-slot sequence?* Producers finish out of order. A single "published" cursor would force
  producer B to wait for A. With per-slot seqs, the consumer simply stops at the first unpublished slot.
- *Why release/acquire, not volatile everywhere?* Release store = "everything I wrote before is visible
  to whoever acquires this value". That's exactly the publication guarantee; full volatile (StoreLoad
  fence) is not needed on that path. On x86 release/acquire are plain movs; the CAS is the only fence.
- *False sharing:* `head` and `tail` are written by different threads; padding puts them on separate
  cache lines so producer CASes don't invalidate the consumer's line.
- *Full ring:* producers spin (back-pressure to the FIX session) rather than drop a tick silently.
- *Tested:* 4 producers x 1M ticks through a 64-slot ring (wraps ~60k times); every field of every tick
  is checked for tearing and per-producer order.

### 4.3 Zero allocation — what it means and how it's proven
GC pauses are the main source of Java tail latency; the fix is to not create garbage on the hot path.
Proven three ways: (1) unit test with `ThreadMXBean.getThreadAllocatedBytes` — 0 bytes over 1M ticks
after warm-up; (2) JMH `-prof gc` — gc.alloc.rate.norm ≈ 0 B/op; (3) soak — hundreds of millions of
ticks, **0 GC collections**. *Be honest about the boundary:* QuickFIX/J parsing allocates (Message
objects, String fields), the AMQP client allocates per publish, the once-a-second snapshot allocates.
The claim is "ring -> book -> pricing allocates nothing". Next step would be a custom FIX parser over a
reused byte buffer (as in my C++ FAST/FIX work at Pacefin).

### 4.4 The idle-strategy story (a real bug I found)
First version: when idle the engine did `parkNanos(50_000)`. Measured latency at 1k ticks/s was
**worse** than at 10k ticks/s (p50 16.7 ms vs 1.6 ms). Root cause: on Windows `parkNanos(50us)`
actually sleeps **~15.8 ms** (the scheduler tick) — I measured it. At low rates the engine was asleep
when ticks arrived. Fix: spin -> yield -> park, **and the producer unparks the engine** if it is
asleep (Disruptor's blocking wait strategy). Lost-wakeup race handled Dekker-style: engine writes
`sleeping=true` then reads `tail`; producer CASes `tail` then reads `sleeping`. Both are volatile
(sequentially consistent), so at least one side sees the other. Result: median wake 62 us; latency at
1k ticks/s went from p50 16.7 ms to ~0.8 ms. (The harness had the same bug — its sender also parked —
which is why it reports its own sender lag.)

### 4.5 Stale providers
Each LP slot stores its last receive time. On every engine loop (including idle wake-ups, at most
1 ms apart) the composite is rebuilt from LPs with `now - lastRecv <= staleMs`. A stale LP's price is
excluded because an LP that stopped updating is no longer firm. Measured: excluded ~202-203 ms after
the last tick with a 200 ms threshold. *Interview angle:* stuck feeds are more dangerous than dead
sessions — the FIX session heartbeats fine while prices freeze.

### 4.6 Pricing
Client bid = best bid − markup, client ask = best ask + markup. Markup = tier base (0.2/0.5/1.0 pip)
+ size band (≤1M: 0, ≤5M: +0.3, >5M: +1.0 pip), in tenths of a pip so JPY works (pip 0.01 vs 0.0001).
Streams carry the smallest band; quotes are priced at their actual size. Crossed composite (best bid ≥
best ask across *different* LPs) is not published — counted instead.

### 4.7 Quotes, TTL and "last look"
A quote = (client, pair, side, qty, price, expiresAt). Trade check (`QuoteCheck`): expired -> reject;
no price -> reject; |current − quoted| > tolerance -> reject (price moved); else fill. Real FX "last
look" is usually **asymmetric** (reject only if the move is against the provider) and controversial
(FX Global Code); mine is symmetric for simplicity — say so.
*Bug I caught:* first version compared in milliseconds but Postgres stores microseconds, so a request at
1000.5 ms vs expiry 1000.9 ms was rejected yet audited as "on time". Now both instants are truncated
to micros and compared at that precision — the check is exact against the stored rows.

### 4.8 Conflation
Two levels: (1) the engine drains a batch and publishes once — 20 queued ticks become one price;
(2) a slow client's queue is declared `x-max-length=1, x-overflow=drop-head`: when a new price arrives
and one is waiting, the broker drops the old one. The client always gets the latest price, the queue
never grows. With prefetch 1 the client is at most 2 prices behind. Measured: conflated queue depth max
1, prices seen ~ms old; a plain queue grew to 10k messages and the client saw prices 8-15 s old.
*Why not conflate everything?* Trades and confirmations must never be conflated — only "latest value
wins" data.

### 4.9 RabbitMQ choices
- Topic exchange, key `price.<PAIR>.<tier>`: clients bind only what they're entitled to. One message
  per tier rather than one with all tiers, because a tier-2 client must not see tier-0 prices.
- Prices: transient, no confirms, raw client with a channel per pair thread (channels aren't
  thread-safe) and a reused byte[] (basicPublish copies it into the frame before returning).
  Dropped (counted) while the broker is down.
- Trades: persistent, publisher confirms, manual acks, **quorum queue** (Raft log; confirms after
  fsync; delivery counter), DLX + DLQ for poison messages, `x-delivery-limit` so a message that always
  fails can't loop forever.
- Ack ordering in `TradeListener`: persist -> publish result and wait for confirm -> ack. A crash before
  the ack means redelivery, which idempotency absorbs.

### 4.10 Exactly-once *effect*
Broker + client are at-least-once (republish unconfirmed batches; redeliver unacked messages). The
consumer makes it exactly-once in effect: lookup by order id (fast path), and the **UNIQUE constraint on
`dedupe_key`** as the real lock (two deliveries racing on two consumer threads: the loser's insert
fails, it reads the winner's row). Replays return the stored answer, so a client asking twice gets the
same answer twice.
*Control run* (`--fx.idempotent=false` sets dedupe_key to a random UUID): duplicate rows, double fills,
and — the interesting one — **conflicting outcomes**: the same order FILLED on first delivery and
REJECTED_PRICE_MOVED on the redelivery 40 s later. Without idempotency a retry can contradict itself.

### 4.11 JPA / Hibernate
- `GenerationType.SEQUENCE` with `allocationSize=50` (pooled optimizer) instead of IDENTITY: IDENTITY
  forces an insert round-trip to learn each id and disables JDBC batching.
- Flyway owns the schema; Hibernate `ddl-auto=validate`. `open-in-view=false`.
- No `@Transactional` around lookup+insert: the single insert is atomic; catching
  `DataIntegrityViolationException` inside a transaction would leave it rollback-only.

### 4.12 MongoDB
Audit records are append-only and variably shaped (N LPs per snapshot) — a document fits; relational
would need a child table per snapshot. `price_snapshots` has a compound index (pair, ts desc) for "what
did we show at T" and a TTL index (7 days) so the collection cleans itself. Quotes are written at issue
and updated with the trade outcome; they're also the fallback lookup if the service restarts between
quote and trade.

### 4.13 FIX
Session layer (Logon 35=A, Heartbeat 0, sequence numbers, resend) vs application layer (Quote 35=S).
`ResetOnLogon=Y` resets sequence numbers each logon — fine for price streams, wrong for orders.
`UseDataDictionary=N` skips per-field validation. Tag 5050 is a user-defined tag (5000-9999) carrying
the send time for measurement. Streaming LP prices also commonly use 35=W/X (market data) or 35=i
(MassQuote). *Story:* QuickFIX/J without an explicit `LogFactory` printed every message to stdout —
220k lines in one run — and dominated the cost until replaced with a quiet log factory.

### 4.14 Measurement method
Latency is measured from each tick's **scheduled** send time (open-loop), so if the sender or system
stalls, the stall is counted (coordinated omission). The harness reports its own sender lag so you can
see how much is harness. Everything runs on one machine; locally the broker is in WSL2, whose
localhost forwarding alone costs ~0.5 ms per hop — the CI (Linux) numbers are the ones to quote.

## 5. Honest scope
- LPs are simulated; one machine; single RabbitMQ node; no auth/TLS; no client session management.
- The FIX parser and AMQP client allocate; zero allocation is claimed for the pricing core only.
- Last look is symmetric. Quotes are stored in memory + MongoDB, not replicated.
- Oracle was **not** used (Postgres only) — don't put Oracle on the resume for this project.

## 6. If asked "what would you do next?"
1. Custom FIX parser over a reused buffer + SBE for internal hops (removes the remaining allocation).
2. Pin engine threads to isolated cores (`taskset`/affinity) and busy-spin; compare tails.
3. Publish throttling per pair (e.g. ≤1 price/ms) — real distribution layers rate-limit per client.
4. Asymmetric last look with a hold time, and per-LP reject-rate tracking.
5. Replace Rabbit for the price fan-out with Aeron/multicast; keep Rabbit for trades.
