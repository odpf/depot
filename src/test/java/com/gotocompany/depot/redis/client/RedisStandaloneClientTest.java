package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.RedisSinkMetrics;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private Jedis jedis;

    @Mock
    private DefaultJedisClientConfig defaultJedisClientConfig;
    @Mock
    private HostAndPort hostAndPort;


    private RedisSinkMetrics redisSinkMetrics;

    @Before
    public void setUp() {
        System.setProperty("SINK_METRICS_APPLICATION_PREFIX", "xyz_");
        RedisSinkConfig sinkConfig = ConfigFactory.create(RedisSinkConfig.class, System.getProperties());
        redisSinkMetrics = new RedisSinkMetrics(sinkConfig);
    }

    @Test
    public void shouldCloseTheClient() throws IOException {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedis, times(1)).close();
    }

    @Test
    public void shouldSendRecordsToJedis() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        Mockito.when(pipeline.exec()).thenReturn(response);
        Object ob = new Object();
        Mockito.when(response.get()).thenReturn(ob);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisStandaloneResponse> responses = new ArrayList<RedisStandaloneResponse>() {{
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> {
                    Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenReturn(responses.get(index));
                    Mockito.when(responses.get(index).process()).thenReturn(responses.get(index));
                }
        );
        List<RedisResponse> actualResponses = redisClient.send(redisRecords);
        verify(pipeline, times(1)).multi();
        verify(pipeline, times(1)).sync();
        verify(instrumentation, times(1)).logDebug("jedis responses: {}", ob);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }

    @Test
    public void shouldInstrumentSuccess() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        Mockito.when(pipeline.exec()).thenReturn(response);
        Object ob = new Object();
        Mockito.when(response.get()).thenReturn(ob);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisStandaloneResponse> responses = new ArrayList<RedisStandaloneResponse>() {{
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> {
                    Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenReturn(responses.get(index));
                    Mockito.when(responses.get(index).process()).thenReturn(responses.get(index));
                }
        );
        redisClient.send(redisRecords);
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_success_response_total", 6L);

    }

    @Test
    public void shouldInstrumentFailure() {

        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};

        IntStream.range(0, redisRecords.size()).forEach(
                index -> Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenThrow(JedisConnectionException.class)
        );
        try {
            redisClient.send(redisRecords);
        } catch (JedisConnectionException ignored) {
        }
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_no_response_total", 6L);

    }

    @Test
    public void shouldInstrumentConnectionRetry() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 1, 2000, redisSinkMetrics);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};

        try {
            redisClient.send(redisRecords);
        } catch (Exception ignored) {
        }
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_connection_retry_total", 1L);


    }

}
