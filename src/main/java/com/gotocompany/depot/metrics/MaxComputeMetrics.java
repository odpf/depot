package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

public class MaxComputeMetrics extends SinkMetrics {

    public static final String MAXCOMPUTE_SINK_PREFIX = "maxcompute_";
    public static final String MAXCOMPUTE_TABLE_TAG = "table=%s";
    public static final String MAXCOMPUTE_PROJECT_TAG = "project=%s";
    public static final String MAXCOMPUTE_API_TAG = "api=%s";
    public static final String MAXCOMPUTE_ERROR_TAG = "error=%s";
    public static final String MAXCOMPUTE_COMPRESSION_TAG = "compression=%s-%s";
    public static final String MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG = "unknown_field_validation_type=%s";

    public MaxComputeMetrics(SinkConfig config) {
        super(config);
    }

    public enum MaxComputeAPIType {
        TABLE_UPDATE,
        TABLE_CREATE,
        TABLE_INSERT,
    }

    public String getMaxComputeOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + MAXCOMPUTE_SINK_PREFIX + "operation_total";
    }

    public String getMaxComputeOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + MAXCOMPUTE_SINK_PREFIX + "operation_latency_milliseconds";
    }

    public String getMaxComputeFlushSizeMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_size_bytes");
    }

    public String getMaxComputeFlushRecordMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_record_count");
    }

    public String getMaxComputeConversionLatencyMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "payload_conversion_latency_milliseconds");
    }

    public String getMaxComputeUnknownFieldValidationLatencyMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "unknown_field_validation_latency_milliseconds");
    }

    public String getMaxComputeStreamingInsertSessionCreatedCount() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "streaming_insert_session_created_count");
    }

    public String getMaxComputeStreamingInsertSessionInitializationLatency() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "streaming_insert_session_initialization_latency_milliseconds");
    }

}
