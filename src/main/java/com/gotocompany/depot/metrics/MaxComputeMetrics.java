package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

public class MaxComputeMetrics extends SinkMetrics {
    public static final String MAXCOMPUTE_SINK_PREFIX = "maxcompute_";

    public MaxComputeMetrics(SinkConfig config) {
        super(config);
    }

    public String getMaxComputeFlushSizeMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_size_bytes");
    }

    public String getMaxComputeFlushRecordMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_record_count");
    }

}
