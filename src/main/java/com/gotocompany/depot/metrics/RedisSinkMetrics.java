package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

public class RedisSinkMetrics extends SinkMetrics {

    public static final String REDIS_SINK_PREFIX = "redis_";

    public RedisSinkMetrics(SinkConfig config) {
        super(config);
    }

    public String getRedisSuccessResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "success_response_total";
    }

    public String getRedisNoResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "no_response_total";
    }

    public String getRedisConnectionRetryTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "connection_retry_total";
    }
}
