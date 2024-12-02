package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionedInsertManagerTest {

    @Test
    public void shouldGroupRecordsBasedOnPartitionSpecAndFlushAll() throws IOException, TunnelException {
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
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig
        );
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);
        int expectedPartitionFlushInvocation = 2;

        partitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(expectedPartitionFlushInvocation))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    @Test
    public void shouldGroupRecordsBasedOnPartitionSpecAndFlushAllWithCompression() throws IOException, TunnelException {
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
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig
        );
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig,
                instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);
        int expectedPartitionFlushInvocation = 2;

        partitionedInsertManager.insert(recordWrappers);

        assertEquals(compressOptionArgumentCaptor.getValue().algorithm, CompressOption.CompressAlgorithm.ODPS_RAW);
        assertEquals(compressOptionArgumentCaptor.getValue().level, 1);
        assertEquals(compressOptionArgumentCaptor.getValue().strategy, 1);
        verify(streamRecordPack, Mockito.times(expectedPartitionFlushInvocation))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
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
        Mockito.doThrow(new IOException())
                .when(streamRecordPack)
                .append(Mockito.any());
        when(streamRecordPack.flush())
                .thenReturn("traceId");
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig
        ));
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig,
                instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);

        partitionedInsertManager.insert(recordWrappers);

        verify(streamingSessionManager, Mockito.times(1))
                .refreshSession(Mockito.anyString());
    }

}
