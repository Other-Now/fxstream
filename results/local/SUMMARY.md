# fxstream results (results/local)

## Tick path (JMH, -prof gc)

| benchmark | LPs | ns/op | alloc B/op |
|---|---|---|---|
| bookRecompute | 4 | 26.6 +- 1.5 | 0.0002 |
| bookRecompute | 8 | 34.8 +- 2.9 | 0.0002 |
| engineTick | 4 | 76.6 +- 4.0 | 0.0005 |
| engineTick | 8 | 82.0 +- 3.0 | 0.0006 |
| ringRoundTrip | 4 | 11.8 +- 0.9 | 0.0001 |
| ringRoundTrip | 8 | 11.6 +- 1.0 | 0.0001 |

## Core soak (no I/O)

625,920,513 ticks in 30 s = **20,857,238 ticks/s** over 4 pair threads (4 producers, 8 CPUs). GC collections during the window: **0**, heap growth 0 B.

## LP tick (FIX) -> client price (RabbitMQ)

| ticks/s | p50 us | p99 us | p99.9 us | max us | prices measured | harness lag p99 us |
|---|---|---|---|---|---|---|
| 1,000 | 821.2 | 1897.5 | 28770.3 | 41779.2 | 18,284 | 60.3 |
| 5,000 | 844.8 | 30572.5 | 42041.3 | 47415.3 | 80,997 | 163.7 |
| 10,000 | 11829.2 | 32374.8 | 38862.8 | 44793.9 | 148,826 | 988.7 |

latency measured from each tick's scheduled send time; price = tier-0 stream; same host

## Bare RabbitMQ publish -> consume

| msgs/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|
| 1,000 | 501.2 | 931.3 | 2738.2 |
| 5,000 | 453.1 | 2152.4 | 21823.5 |
| 15,000 | 819.2 | 40042.5 | 46039.0 |

## Stale provider exclusion

Threshold 200 ms, 20 trials: last tick -> excluded min 202.4 / p50 202.8 / max 203.4 ms.

## Slow consumer: conflated vs plain queue

Stream 1,000 ticks/s, client handles 50/s, 20 s.

| queue | max depth | price age p50 ms | p99 ms | ends on latest |
|---|---|---|---|---|
| conflated (max-length 1, drop-head) | 1 | 1.6 | 5.3 | True |
| plain | 13,761 | 10343.2 | 20535.3 | backlog 13,783 |

## Quote expiry

1000 quotes, TTL 200 ms, traded uniform 0..400 ms after the quote: 610 arrived late, **610 rejected (100.0%)**, late fills 0, on-time wrongly rejected 0.

## Trades across broker kill -9 / restart

| run | idempotent | orders | kills | lost | duplicate rows | double fills | conflicting outcomes | replays caught | DLQ |
|---|---|---|---|---|---|---|---|---|---|
| chaos-safe | True | 15,000 | 5 | 0 | 0 | 0 | 0 | 30 | 0 |
| chaos-unsafe | False | 15,000 | 5 | 0 | 57 | 36 | 20 | 0 | 0 |

