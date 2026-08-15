/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.sinks.Sink;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real RocksDB-backed KV sink. Extracts a key from each incoming row
 * using a dotted-path expression; value is the row's JSON serialisation
 * in UTF-8. Physical DB lives under {@code &lt;home&gt;/kv/&lt;name&gt;}.
 *
 * <p>Registered via ServiceLoader — drop this jar on the classpath and
 * every {@code kind: kvstore} sink upgrades from the TSV stub to a real
 * RocksDB write path.</p>
 */
public final class RocksDbKvStoreSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final String keyExpr;
    private final Path   dir;
    private final AtomicLong n = new AtomicLong();
    private RocksDbSharedRegistry.Entry entry;
    /**
     * Per-task highest-seq-seen. Retried tasks with lower seqs are
     * skipped (idempotent replay). Ephemeral to this sink instance —
     * a subsequent job with a fresh task id starts at 0 again.
     */
    private final java.util.Map<String, Long> taskSeqSeen = new java.util.concurrent.ConcurrentHashMap<>();

    public RocksDbKvStoreSink(String name, String keyExpr, Path home) {
        this.name    = name;
        this.keyExpr = keyExpr == null ? "id" : keyExpr;
        this.dir     = home.resolve("kv").resolve(name);
    }

    @Override
    public void open() throws Exception {
        entry = RocksDbSharedRegistry.acquire(name, dir);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (entry == null) open();
        String key = extractKey(row, keyExpr);
        entry.db.put(key.getBytes(StandardCharsets.UTF_8),
                     JSON.writeValueAsBytes(row));
        n.incrementAndGet();
    }

    /**
     * Idempotent write — dedupes by ({@code taskId}, {@code seq}) so a
     * retried task's re-played rows land at most once. The scheduler
     * hands each row a monotonic per-task seq; if we've already seen
     * a seq >= this one for the same task, skip.
     */
    @Override
    public void addIdempotent(String taskId, long seq, JsonNode row) throws Exception {
        if (taskId == null) { add(row); return; }
        Long prev = taskSeqSeen.get(taskId);
        if (prev != null && prev >= seq) return;    // already processed
        taskSeqSeen.put(taskId, seq);
        add(row);
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() {
        if (entry != null) RocksDbSharedRegistry.release(entry);
    }

    private static String extractKey(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return "<null>";
            cur = cur.get(seg);
        }
        return (cur == null || cur.isNull()) ? "<null>" : cur.asText();
    }
}
