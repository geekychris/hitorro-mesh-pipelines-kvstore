/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;
import com.hitorro.mesh.pipelines.sources.SourceAdapter;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.core.iterator.sinks.kvstore.KvStoreSink;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ServiceLoader glue for the {@code kvstore} sink + source kinds. The
 * actual sink implementation lives in {@code hitorro-kvstore} as
 * {@link KvStoreSink} — reusable outside the mesh pipeline runtime.
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
    public Sink<JsonNode> create(SinkSpec spec, Path home) {
        SinkSpec.KvStore s = (SinkSpec.KvStore) spec;
        return new KvStoreSink(s.name(), s.keyExpr(), home);
    }

    @Override
    public Iterator<JsonNode> open(SourceSpec spec, Path home, AtomicBoolean cancelled) throws Exception {
        SourceSpec.KvStore s = (SourceSpec.KvStore) spec;
        return new com.hitorro.util.core.iterator.sources.kvstore.KvStoreSource(s.name(), home);
    }
}
