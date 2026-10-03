package com.fxstream.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.HdrHistogram.Histogram;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Small shared helpers: result files, histogram summaries, broker connections, REST calls. */
final class Out {
    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    static final HttpClient HTTP = HttpClient.newHttpClient();

    private Out() {}

    /** p50/p90/p99/p99.9/max in microseconds from a nanosecond histogram. */
    static Map<String, Object> micros(Histogram h) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", h.getTotalCount());
        m.put("p50_us", us(h.getValueAtPercentile(50)));
        m.put("p90_us", us(h.getValueAtPercentile(90)));
        m.put("p99_us", us(h.getValueAtPercentile(99)));
        m.put("p99_9_us", us(h.getValueAtPercentile(99.9)));
        m.put("max_us", us(h.getMaxValue()));
        return m;
    }

    static Map<String, Object> millis(Histogram h) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", h.getTotalCount());
        m.put("min_ms", ms(h.getMinValue()));
        m.put("p50_ms", ms(h.getValueAtPercentile(50)));
        m.put("p99_ms", ms(h.getValueAtPercentile(99)));
        m.put("max_ms", ms(h.getMaxValue()));
        return m;
    }

    private static double us(long nanos) { return Math.round(nanos / 100.0) / 10.0; }
    private static double ms(long nanos) { return Math.round(nanos / 100_000.0) / 10.0; }

    static void write(Main.Args a, String name, Object result) throws Exception {
        Path dir = Path.of(a.out());
        Files.createDirectories(dir);
        Path f = dir.resolve(name + ".json");
        Files.writeString(f, JSON.writeValueAsString(result));
        System.out.println(JSON.writeValueAsString(result));
        System.out.println("-> " + f);
    }

    static Connection rabbit(Main.Args a, String name) throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost(a.host());
        cf.setPort(a.i("amqp-port", 5672));
        cf.setUsername(a.str("amqp-user", "guest"));
        cf.setPassword(a.str("amqp-pass", "guest"));
        cf.setAutomaticRecoveryEnabled(true);
        cf.setNetworkRecoveryInterval(500);
        return cf.newConnection(name);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> getJson(String url) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return JSON.readValue(r.body(), Map.class);
    }

    @SuppressWarnings("unchecked")
    static java.util.List<Map<String, Object>> getJsonList(String url) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return JSON.readValue(r.body(), java.util.List.class);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> postJson(String url, Object body) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() / 100 != 2) throw new IllegalStateException(r.statusCode() + " " + r.body());
        return JSON.readValue(r.body(), Map.class);
    }

    /** Difference of two /api/stats snapshots, numeric fields only. */
    static Map<String, Object> delta(Map<String, Object> before, Map<String, Object> after) {
        Map<String, Object> d = new LinkedHashMap<>();
        after.forEach((k, v) -> {
            if (v instanceof Number n && before.get(k) instanceof Number b) d.put(k, n.longValue() - b.longValue());
        });
        return d;
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
