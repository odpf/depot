package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.data.Record;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MaxComputeSinkTest {

    @Test
    public void shouldInsertMaxComputeSinkTest() throws SinkException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        Mockito.when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        Mockito.when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        Mockito.when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        MaxComputeClient maxComputeClient = Mockito.spy(new MaxComputeClient(maxComputeSinkConfig));
        Mockito.doNothing()
                .when(maxComputeClient)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        MaxComputeSink maxComputeSink = new MaxComputeSink(maxComputeClient, messageRecordConverter);
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 0, null, null));
        List<RecordWrapper> invalidRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 1,
                new ErrorInfo(new RuntimeException("Invalid Schema"), ErrorType.DESERIALIZATION_ERROR), null));
        Mockito.when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, invalidRecords));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Mockito.verify(maxComputeClient, Mockito.times(1)).insert(validRecords);
        Assertions.assertEquals(1, sinkResponse.getErrors().size());
        Assertions.assertEquals(sinkResponse.getErrors().get(1L), new ErrorInfo(new RuntimeException("Invalid Schema"), ErrorType.DESERIALIZATION_ERROR));
    }

    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowError() throws SinkException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        Mockito.when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        Mockito.when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        Mockito.when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        MaxComputeClient maxComputeClient = Mockito.spy(new MaxComputeClient(maxComputeSinkConfig));
        Mockito.doNothing()
                .when(maxComputeClient)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        Mockito.doThrow(new RuntimeException("Insert failed"))
                .when(maxComputeClient)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(maxComputeClient, messageRecordConverter);
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        Mockito.when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.DEFAULT_ERROR.equals(s.getErrorType())));
    }

}
