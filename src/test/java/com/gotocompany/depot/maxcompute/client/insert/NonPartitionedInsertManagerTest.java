package com.gotocompany.depot.maxcompute.client.insert;


import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NonPartitionedInsertManagerTest {

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void shouldFlushAllTheRecords() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        when(streamUploadSession.newRecordPack())
                .thenReturn(streamRecordPack);
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        );
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    @Test
    public void shouldFlushAllTheRecordsWithCompressOption() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        );
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
        assertEquals(compressOptionArgumentCaptor.getValue().algorithm, CompressOption.CompressAlgorithm.ODPS_RAW);
        assertEquals(compressOptionArgumentCaptor.getValue().strategy, 1);
        assertEquals(compressOptionArgumentCaptor.getValue().level, 1);
    }

    @Test(expected = IOException.class)
    public void shouldRefreshSessionWhenIOExceptionOccurred() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        Mockito.doThrow(IOException.class)
                .when(streamRecordPack)
                .append(Mockito.any());
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        ));
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        verify(streamingSessionManager, Mockito.times(1))
                .refreshSession(Mockito.any());
    }

}
