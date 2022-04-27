package io.odpf.sink.connectors.metrics;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatsDReporter implements Closeable {

    private final StatsDClient client;
    private final String globalTags;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporter.class);

    public StatsDReporter(StatsDClient client, String... globalTags) {
        this.client = client;
        this.globalTags = String.join(",", globalTags).replaceAll(":", "=");
    }

    public StatsDClient getClient() {
        return client;
    }

    public void captureCount(String metric, Long delta, String... tags) {
        client.count(withTags(metric, tags), delta);
    }

    public void captureHistogram(String metric, long delta, String... tags) {
        client.time(withTags(metric, tags), delta);
    }

    public void captureDurationSince(String metric, Instant startTime, String... tags) {
        client.recordExecutionTime(withTags(metric, tags), Duration.between(startTime, Instant.now()).toMillis());
    }

    public void captureDuration(String metric, long duration, String... tags) {
        client.recordExecutionTime(withTags(metric, tags), duration);
    }

    public void gauge(String metric, Integer value, String... tags) {
        client.gauge(withTags(metric, tags), value);
    }

    public void increment(String metric, String... tags) {
        captureCount(metric, 1L, tags);
    }

    public void recordEvent(String metric, String eventName, String... tags) {
        client.recordSetValue(withTags(metric, tags), eventName);
    }

    private String withGlobalTags(String metric) {
        return metric + "," + this.globalTags;
    }

    private String withTags(String metric, String... tags) {
        return Stream.concat(Stream.of(withGlobalTags(metric)), Stream.of(tags))
                .collect(Collectors.joining(","));
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("StatsD connection closed");
        client.stop();
    }

}
