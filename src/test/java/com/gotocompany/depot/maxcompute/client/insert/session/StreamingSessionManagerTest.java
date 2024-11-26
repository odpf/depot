package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class StreamingSessionManagerTest {

    @Test
    public void shouldCreateNewPartitionedSessionIfCacheIsEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        Mockito.when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.partitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        Mockito.verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        Assertions.assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    @Test
    public void shouldReturnSameInstanceIfCacheNotEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        Mockito.when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.partitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        Mockito.verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        Assertions.assertEquals(streamUploadSessionMock, streamUploadSession);
        Assertions.assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    @Test
    public void shouldEvictOldLatestInstanceWhenCapacityExceeded() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        Mockito.when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.partitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("different_test_session");

        Mockito.verify(tableTunnel, Mockito.times(2))
                .buildStreamUploadSession("test_project", "test_table");
        Assertions.assertEquals(streamUploadSessionMock, streamUploadSession);
        Assertions.assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    @Test
    public void shouldCreateNewNonPartitionedSessionIfCacheIsEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.nonParititonedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        Mockito.verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        Assertions.assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    @Test
    public void shouldReturnSameNonPartitionedSessionIfCacheNotEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        Mockito.when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.nonParititonedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        Mockito.verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        Assertions.assertEquals(streamUploadSessionMock, streamUploadSession);
        Assertions.assertEquals(streamUploadSession, secondStreamUploadSession);
    }

}
