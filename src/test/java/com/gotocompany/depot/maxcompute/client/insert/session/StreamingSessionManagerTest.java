package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingSessionManagerTest {

    @Test
    public void shouldCreateNewPartitionedSessionIfCacheIsEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    @Test
    public void shouldReturnSameInstanceIfCacheNotEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    @Test
    public void shouldEvictOldLatestInstanceWhenCapacityExceeded() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("different_test_session");

        verify(tableTunnel, Mockito.times(2))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    @Test
    public void shouldCreateNewNonPartitionedSessionIfCacheIsEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    @Test
    public void shouldReturnSameNonPartitionedSessionIfCacheNotEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    @Test
    public void shouldReturnRefreshTheSession() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig);

        nonPartitionedStreamingSessionManager.getSession("test_session");
        nonPartitionedStreamingSessionManager.refreshSession("test_session");

        verify(tableTunnel, Mockito.times(2))
                .buildStreamUploadSession("test_project", "test_table");
    }

}
