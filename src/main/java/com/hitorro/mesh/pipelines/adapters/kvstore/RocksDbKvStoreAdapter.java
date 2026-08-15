/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.Sink;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;
import com.hitorro.mesh.pipelines.sources.SourceAdapter;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ServiceLoader-registered adapter for the {@code kvstore} sink + source
 * kinds. Overrides the built-in Phase-1 TSV stub with real RocksDB
 * reads/writes.
 */
public final class RocksDbKvStoreAdapter implements SinkAdapter, SourceAdapter {

    @Override
    public boolean handles(SinkSpec spec) {
        return spec instanceof SinkSpec.KvStore;
    }

    @Override
    public boolean handles(SourceSpec spec) {
        return spec instanceof SourceSpec.KvStore;
    }

    @Override
    public Sink create(SinkSpec spec, Path home) {
        SinkSpec.KvStore s = (SinkSpec.KvStore) spec;
        return new RocksDbKvStoreSink(s.name(), s.keyExpr(), home);
    }

    @Override
    public Iterator<JsonNode> open(SourceSpec spec, Path home, AtomicBoolean cancelled) throws Exception {
        SourceSpec.KvStore s = (SourceSpec.KvStore) spec;
        return new RocksDbKvStoreSource(s.name(), home);
    }
}
