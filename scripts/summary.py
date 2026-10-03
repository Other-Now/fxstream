"""Render the JSON results in a directory as one Markdown summary (used for the CI job summary)."""
import json
import os
import sys

d = sys.argv[1] if len(sys.argv) > 1 else "results/local"


def load(name):
    p = os.path.join(d, name + ".json")
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None


out = [f"# fxstream results ({d})", ""]

jmh = load("jmh")
if jmh:
    out += ["## Tick path (JMH, -prof gc)", "", "| benchmark | LPs | ns/op | alloc B/op |", "|---|---|---|---|"]
    for r in jmh:
        out.append("| {} | {} | {:.1f} +- {:.1f} | {:.4f} |".format(
            r["benchmark"].split(".")[-1], r["params"]["lps"], r["primaryMetric"]["score"],
            r["primaryMetric"]["scoreError"], r["secondaryMetrics"]["gc.alloc.rate.norm"]["score"]))
    out.append("")

s = load("soak")
if s:
    out += ["## Core soak (no I/O)", "",
            f"{s['ticks']:,} ticks in {s['seconds']} s = **{s['ticks_per_s']:,} ticks/s** over {s['pairs']} pair threads "
            f"({s['producers']} producers, {s['cpus']} CPUs). GC collections during the window: **{s['gc_collections']}**, "
            f"heap growth {s['heap_used_growth_bytes']} B.", ""]

lat = load("latency")
if lat:
    out += ["## LP tick (FIX) -> client price (RabbitMQ)", "",
            "| ticks/s | p50 us | p99 us | p99.9 us | max us | prices measured | harness lag p99 us |", "|---|---|---|---|---|---|---|"]
    for r in lat["runs"]:
        t, g = r["tick_to_client_price"], r["harness_sender_lag"]
        out.append(f"| {r['ticks_per_s']:,.0f} | {t['p50_us']} | {t['p99_us']} | {t['p99_9_us']} | {t['max_us']} | {t['count']:,} | {g['p99_us']} |")
    out += ["", lat["note"], ""]

am = load("amqp")
if am:
    out += ["## Bare RabbitMQ publish -> consume", "", "| msgs/s | p50 us | p99 us | p99.9 us |", "|---|---|---|---|"]
    for r in am["runs"]:
        t = r["publish_to_consume"]
        out.append(f"| {r['msgs_per_s']:,.0f} | {t['p50_us']} | {t['p99_us']} | {t['p99_9_us']} |")
    out.append("")

st = load("stale")
if st:
    x = st["last_tick_to_exclusion"]
    out += ["## Stale provider exclusion", "",
            f"Threshold {st['stale_threshold_ms']} ms, {st['trials']} trials: last tick -> excluded "
            f"min {x['min_ms']} / p50 {x['p50_ms']} / max {x['max_ms']} ms.", ""]

c = load("conflation")
if c:
    a, b = c["conflated"], c["plain"]
    out += ["## Slow consumer: conflated vs plain queue", "",
            f"Stream {c['stream_ticks_per_s']:,.0f} ticks/s, client handles {c['client_capacity_per_s']}/s, {c['seconds']} s.", "",
            "| queue | max depth | price age p50 ms | p99 ms | ends on latest |", "|---|---|---|---|---|",
            f"| conflated (max-length 1, drop-head) | {a['max_queue_depth']} | {a['price_age_seen_by_client']['p50_ms']} | {a['price_age_seen_by_client']['p99_ms']} | {a['ends_on_latest_price']} |",
            f"| plain | {b['max_queue_depth']:,} | {b['price_age_seen_by_client']['p50_ms']} | {b['price_age_seen_by_client']['p99_ms']} | backlog {b['backlog_at_end']:,} |", ""]

e = load("expiry")
if e:
    out += ["## Quote expiry", "",
            f"{e['quotes']} quotes, TTL {e['ttl_ms']} ms, traded {e['trade_delay']}: {e['late_requests']} arrived late, "
            f"**{e['late_rejected_expired']} rejected ({e['pct_late_rejected']}%)**, late fills {e['late_filled']}, "
            f"on-time wrongly rejected {e['on_time_wrongly_rejected_expired']}.", ""]

rows = [(n, load(n)) for n in ("chaos-safe", "chaos-unsafe")]
if any(r for _, r in rows):
    out += ["## Trades across broker kill -9 / restart", "",
            "| run | idempotent | orders | kills | lost | duplicate rows | double fills | conflicting outcomes | replays caught | DLQ |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for n, r in rows:
        if r:
            out.append(f"| {n} | {r['idempotent']} | {r['orders']:,} | {r['broker_kills']} | {r['lost_orders']} | {r['duplicate_rows']} | "
                       f"{r['double_fills']} | {r.get('orders_with_conflicting_outcomes', '-')} | {r['service'].get('tradesReplayed', 0)} | {r['dlq_depth']} |")
    out.append("")

sys.stdout.reconfigure(encoding="utf-8")
print("\n".join(out))
