package com.gotocompany.depot;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.MaxComputeSinkFactory;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import deduction.HttpRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Main {
    public static void main(String[] args) throws SinkException {
        StatsDReporter statsDReporter = new StatsDReporter(new NoOpStatsDClient());
        MaxComputeSinkFactory maxComputeSinkFactory = new MaxComputeSinkFactory(statsDReporter, getMockEnv());
        maxComputeSinkFactory.init();
        Sink sink = maxComputeSinkFactory.create();

        while (true) {
            int batchCount = Math.max(1, new Random().nextInt() % 20);

            List<Message> messageList = IntStream.range(0, batchCount)
                    .mapToObj(index -> {
                        byte[] messageBytes = HttpRequest.newBuilder()
                                .setField1(UUID.randomUUID().toString())
                                .setField2(UUID.randomUUID().toString())
                                .setEventTimestamp(Timestamp.newBuilder()
                                        .setSeconds(System.currentTimeMillis() / 1000)
                                        .setNanos(0)
                                        .build())
                                .build().toByteArray();
                        return new Message(null, messageBytes, new Tuple<>("topic", "test"), new Tuple<>("partition", 0), new Tuple<>("offset", index), new Tuple<>("timestamp", System.currentTimeMillis()));
                    })
                    .collect(Collectors.toList());
            sink.pushToSink(messageList);
            log.info("Pushed {} messages to sink", batchCount);
        }
    }

    private static Map<String, String> getMockEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE");
        env.put("INPUT_SCHEMA_PROTO_CLASS", "deduction.HttpRequest");
        env.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "deduction.HttpRequest");
        env.put("SCHEMA_REGISTRY_STENCIL_ENABLE", "true");
        env.put("SCHEMA_REGISTRY_STENCIL_URLS", "http://stencil.integration.gtfdata.io/v1beta1/namespaces/gtfn/schemas/depot");
        env.put("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "10000");
        env.put("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "true");
        env.put("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY", "LONG_POLLING");
        env.put("SINK_MAXCOMPUTE_ODPS_URL", "http://service.ap-southeast-5.maxcompute.aliyun.com/api");
        env.put("SINK_MAXCOMPUTE_TUNNEL_URL", "http://dt.ap-southeast-5.maxcompute.aliyun.com");
        env.put("SINK_MAXCOMPUTE_ACCESS_ID", "");
        env.put("SINK_MAXCOMPUTE_ACCESS_KEY", "");
        env.put("SINK_MAXCOMPUTE_PROJECT_ID", "goto_test");
        env.put("SINK_MAXCOMPUTE_SCHEMA", "default");
        env.put("SINK_MAXCOMPUTE_METADATA_NAMESPACE", "__kafka_metadata");
        env.put("SINK_MAXCOMPUTE_ADD_METADATA_ENABLED", "true");
        env.put("SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES", "timestamp=timestamp,topic=string,partition=integer,offset=long");
        env.put("SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE", "true");
        env.put("SINK_MAXCOMPUTE_TABLE_PARTITION_KEY", "event_timestamp");
        env.put("SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME", "__partition_key");
        env.put("SINK_MAXCOMPUTE_TABLE_NAME", "depot_test_partitioned_1");
        env.put("SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS", "100");
        return env;
    }
}
