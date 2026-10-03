# fxstream results (results/ci)

## Tick path (JMH, -prof gc)

| benchmark | LPs | ns/op | alloc B/op |
|---|---|---|---|
| bookRecompute | 4 | 21.8 +- 4.3 | 0.0001 |
| bookRecompute | 8 | 38.3 +- 5.3 | 0.0003 |
| engineTick | 4 | 51.6 +- 8.0 | 0.0004 |
| engineTick | 8 | 59.6 +- 11.3 | 0.0004 |
| ringRoundTrip | 4 | 8.4 +- 2.0 | 0.0001 |
| ringRoundTrip | 8 | 8.1 +- 2.0 | 0.0001 |

## Core soak (no I/O)

181,013,094 ticks in 30 s = **6,033,752 ticks/s** over 4 pair threads (4 producers, 4 CPUs). GC collections during the window: **0**, heap growth 0 B.

## LP tick (FIX) -> client price (RabbitMQ)

| ticks/s | p50 us | p99 us | p99.9 us | max us | prices measured | harness lag p99 us |
|---|---|---|---|---|---|---|
| 1,000 | 510.7 | 11763.7 | 51806.2 | 69926.9 | 22,595 | 139.8 |
| 5,000 | 1122.3 | 232128.5 | 320864.3 | 372768.8 | 89,006 | 884.2 |
| 10,000 | 281280.5 | 1246756.9 | 1373634.6 | 1394606.1 | 136,092 | 2353.2 |
| 20,000 | 1866465.3 | 3170893.8 | 3250585.6 | 3284140.0 | 142,469 | 5656.6 |

latency measured from each tick's scheduled send time; price = tier-0 stream; same host

## Bare RabbitMQ publish -> consume

| msgs/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|
| 1,000 | 285.4 | 328990.7 | 489947.1 |
| 5,000 | 283.4 | 16826.4 | 94044.2 |
| 15,000 | 992.8 | 245235.7 | 322699.3 |

## Stale provider exclusion

Threshold 200 ms, 20 trials: last tick -> excluded min 200.7 / p50 201.3 / max 202.9 ms.

## Slow consumer: conflated vs plain queue

Stream 1,000 ticks/s, client handles 50/s, 20 s.

| queue | max depth | price age p50 ms | p99 ms | ends on latest |
|---|---|---|---|---|
| conflated (max-length 1, drop-head) | 1 | 1.3 | 5.4 | True |
| plain | 14,803 | 10125.0 | 19931.3 | backlog 14,809 |

## Quote expiry

1000 quotes, TTL 200 ms, traded uniform 0..400 ms after the quote: 534 arrived late, **534 rejected (100.0%)**, late fills 0, on-time wrongly rejected 0.

## Trades across broker kill -9 / restart

| run | idempotent | orders | kills | lost | duplicate rows | double fills | conflicting outcomes | replays caught | DLQ |
|---|---|---|---|---|---|---|---|---|---|
| chaos-safe | True | 15,000 | 6 | 0 | 0 | 0 | 0 | 21 | 0 |
| chaos-unsafe | False | 15,000 | 6 | 0 | 16 | 0 | 10 | 0 | 0 |

