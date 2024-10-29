package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PartitionedInsertManagerTest {

    @Test
    public void shouldGroupRecordsBasedOnPartitionSpecAndFlushAll() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        Mockito.when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        Mockito.when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        Mockito.when(streamUploadSession.newRecordPack())
                .thenReturn(streamRecordPack);
        Mockito.when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.setPartitionSpec(Mockito.any(PartitionSpec.class)))
                .thenReturn(builder);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        Mockito.when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeout())
                .thenReturn(1000L);
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        Mockito.when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        Mockito.when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(tableTunnel, maxComputeSinkConfig);
        int expectedPartitionFlushInvocation = 2;

        partitionedInsertManager.insert(recordWrappers);

        Mockito.verify(streamRecordPack, Mockito.times(expectedPartitionFlushInvocation))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }


}
