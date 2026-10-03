package com.fxstream.bench;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Harness entry point. Everything except {@code jmh} and {@code soak} drives a running fxstream service
 * from the outside: FIX in, RabbitMQ out, REST for quotes, JDBC to check what was stored.
 *
 * <pre>
 *   jmh  [jmh args]            microbenchmarks of the tick path (pass -prof gc for allocation)
 *   soak                       core only, no I/O: sustained ticks and a GC count
 *   latency                    LP tick (FIX) -> client price (RabbitMQ), at several rates
 *   amqp                       bare RabbitMQ publish -> consume latency, for comparison
 *   stale                      time to drop a provider that goes quiet
 *   conflation                 slow consumer: conflated queue vs plain queue
 *   expiry                     trades against quotes at random ages around the TTL
 *   chaos                      trades while the broker is kill -9'd and restarted
 * </pre>
 */
public final class Main {
    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("usage: <jmh|soak|latency|amqp|stale|conflation|expiry|chaos> [--key value]...");
            System.exit(2);
        }
        String cmd = argv[0];
        String[] rest = Arrays.copyOfRange(argv, 1, argv.length);
        if (cmd.equals("jmh")) {
            org.openjdk.jmh.Main.main(rest);
            return;
        }
        Args a = Args.parse(rest);
        switch (cmd) {
            case "soak" -> Soak.run(a);
            case "latency" -> LatencyBench.tickToPrice(a);
            case "amqp" -> LatencyBench.amqpOnly(a);
            case "stale" -> StaleBench.run(a);
            case "conflation" -> ConflationBench.run(a);
            case "expiry" -> TradeBench.expiry(a);
            case "chaos" -> TradeBench.chaos(a);
            default -> {
                System.err.println("unknown command " + cmd);
                System.exit(2);
            }
        }
        System.exit(0); // QuickFIX/J and amqp-client leave non-daemon threads behind
    }

    record Args(Map<String, String> m) {
        static Args parse(String[] a) {
            Map<String, String> m = new HashMap<>();
            for (int i = 0; i + 1 < a.length; i += 2) m.put(a[i].replaceFirst("^--", ""), a[i + 1]);
            return new Args(m);
        }

        String str(String k, String d) { return m.getOrDefault(k, d); }
        int i(String k, int d) { return m.containsKey(k) ? Integer.parseInt(m.get(k)) : d; }
        long l(String k, long d) { return m.containsKey(k) ? Long.parseLong(m.get(k)) : d; }
        String host() { return str("host", "localhost"); }
        String api() { return str("api", "http://localhost:8090"); }
        String out() { return str("out", "results"); }
    }
}
