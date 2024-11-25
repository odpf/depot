package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamingSessionManagerFactoryTest {

    @Test
    public void shouldReturnPartitionedStreamingSessionManager() {
        TableTunnel tableTunnel = mock(TableTunnel.class);
        MaxComputeSinkConfig maxComputeSinkConfig = mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);

        StreamingSessionManager streamingSessionManager = StreamingSessionManagerFactory.createStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        Assertions.assertTrue(streamingSessionManager instanceof PartitionedStreamingSessionManager);
    }

    @Test
    public void shouldReturnNonPartitionedStreamingSessionManager() {
        TableTunnel tableTunnel = mock(TableTunnel.class);
        MaxComputeSinkConfig maxComputeSinkConfig = mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);

        StreamingSessionManager streamingSessionManager = StreamingSessionManagerFactory.createStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        Assertions.assertTrue(streamingSessionManager instanceof NonPartitionedStreamingSessionManager);
    }

}
