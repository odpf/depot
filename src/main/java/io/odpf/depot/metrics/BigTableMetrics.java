package io.odpf.depot.metrics;

import io.odpf.depot.config.OdpfSinkConfig;

public class BigTableMetrics extends SinkMetrics {

    public static final String BIGTABLE_SINK_PREFIX = "bigtable_";
    public static final String BIGTABLE_INSTANCE_TAG = "instance=%s";
    public static final String BIGTABLE_TABLE_TAG = "table=%s";
    public BigTableMetrics(OdpfSinkConfig config) {
        super(config);
    }

    public String getBigtableOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_latency_milliseconds";
    }

    public String getBigtableOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_total";
    }
}
