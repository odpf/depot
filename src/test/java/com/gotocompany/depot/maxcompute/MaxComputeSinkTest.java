package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

public class MaxComputeSinkTest {

    @Test
    public void shouldInsertMaxComputeSinkTest() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doNothing()
                .when(insertManager)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 0, null, null));
        List<RecordWrapper> invalidRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 1,
                new ErrorInfo(new RuntimeException("Invalid Schema"), ErrorType.DESERIALIZATION_ERROR), null));
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, invalidRecords));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Mockito.verify(insertManager, Mockito.times(1)).insert(validRecords);
        Assertions.assertEquals(1, sinkResponse.getErrors().size());
        Assertions.assertEquals(sinkResponse.getErrors().get(1L).getException().getMessage(), "Invalid Schema");
        Assertions.assertEquals(sinkResponse.getErrors().get(1L).getErrorType(), ErrorType.DESERIALIZATION_ERROR);
    }

    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowTunnelExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new TunnelException("Failed establishing connection"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.SINK_RETRYABLE_ERROR.equals(s.getErrorType())));
    }

    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowIOExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new IOException("Failed flushing"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.SINK_RETRYABLE_ERROR.equals(s.getErrorType())));
    }

    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new RuntimeException("Unexpected Error"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.DEFAULT_ERROR.equals(s.getErrorType())));
    }

    @Test
    public void shouldDoNothing() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);

        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));

        assertDoesNotThrow(maxComputeSink::close);
    }

}
