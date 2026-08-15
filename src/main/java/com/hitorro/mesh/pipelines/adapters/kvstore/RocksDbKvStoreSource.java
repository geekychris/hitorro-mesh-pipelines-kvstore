/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.RocksIterator;

import java.nio.file.Path;
import java.util.Iterator;

/**
 * Read-side companion: iterates every entry in a named RocksDB KV store
 * and yields the JSON value as a row. Enables the classic "one job
 * writes into the KV store, a downstream job reads back" pattern.
 */
public final class RocksDbKvStoreSource implements Iterator<JsonNode>, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final RocksDbSharedRegistry.Entry entry;
    private final RocksIterator it;
    private boolean advanced;

    public RocksDbKvStoreSource(String name, Path home) throws Exception {
        this.entry = RocksDbSharedRegistry.acquire(name,
                home.resolve("kv").resolve(name));
        this.it = entry.db.newIterator();
        this.it.seekToFirst();
    }

    @Override
    public boolean hasNext() {
        return it.isValid();
    }

    @Override
    public JsonNode next() {
        if (!it.isValid()) throw new java.util.NoSuchElementException();
        byte[] v = it.value();
        try {
            JsonNode row = JSON.readTree(v);
            it.next();
            return row;
        } catch (java.io.IOException e) {
            throw new RuntimeException("bad kv value: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try { it.close(); } catch (Exception ignored) { }
        RocksDbSharedRegistry.release(entry);
    }
}
