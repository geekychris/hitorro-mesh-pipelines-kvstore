/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.Sink;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import com.hitorro.mesh.pipelines.sources.SourceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDbRoundTripTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void write_via_sink_read_back_via_source(@TempDir Path home) throws Exception {
        SinkRegistry reg = new SinkRegistry(home);
        // Adapter is picked up via META-INF/services in this same jar.
        try (Sink sink = reg.create(new SinkSpec.KvStore("cities", "iso3"))) {
            sink.open();
            sink.add(JSON.readTree("{\"iso3\":\"USA\",\"name\":\"United States\"}"));
            sink.add(JSON.readTree("{\"iso3\":\"CHN\",\"name\":\"China\"}"));
            sink.add(JSON.readTree("{\"iso3\":\"IND\",\"name\":\"India\"}"));
            assertThat(sink.count()).isEqualTo(3);
        }
        // Physical files must exist under home/kv/cities/
        assertThat(home.resolve("kv/cities/CURRENT")).exists();

        SourceFactory sf = new SourceFactory(reg);
        List<JsonNode> read = new ArrayList<>();
        Iterator<JsonNode> it = sf.open(new SourceSpec.KvStore("cities"), new AtomicBoolean());
        try {
            while (it.hasNext()) read.add(it.next());
        } finally {
            if (it instanceof AutoCloseable c) c.close();
        }
        assertThat(read).hasSize(3);
        assertThat(read).extracting(r -> r.get("iso3").asText())
                .containsExactlyInAnyOrder("USA", "CHN", "IND");
    }
}
