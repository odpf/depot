package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.maxcompute.record.ProtoDataColumnRecordDecorator;
import com.gotocompany.depot.maxcompute.record.ProtoMetadataColumnRecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class ProtoMessageRecordConverterTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private ConverterOrchestrator converterOrchestrator;
    private ProtoMessageParser protoMessageParser;
    private MaxComputeSchemaHelper maxComputeSchemaHelper;
    private SinkConfig sinkConfig;
    private MaxComputeSchemaCache maxComputeSchemaCache;
    private ProtoMessageRecordConverter protoMessageRecordConverter;

    @Before
    public void setup() throws IOException {
        maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_column");
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        converterOrchestrator = new ConverterOrchestrator(maxComputeSinkConfig);
        protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        ParsedMessage parsedMessage = Mockito.mock(ParsedMessage.class);
        Mockito.when(parsedMessage.getRaw()).thenReturn(getMockedMessage());
        Mockito.when(protoMessageParser.parse(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(parsedMessage);
        sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                converterOrchestrator,
                maxComputeSinkConfig,
                descriptor
        );
        maxComputeSchemaHelper = new MaxComputeSchemaHelper(converterOrchestrator, maxComputeSinkConfig, partitioningStrategy);
        maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);
        Mockito.when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        partitioningStrategy.setMaxComputeSchemaCache(maxComputeSchemaCache);
        RecordDecorator protoDataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null,
                converterOrchestrator,
                protoMessageParser, sinkConfig, partitioningStrategy);
        RecordDecorator metadataColumnRecordDecorator = new ProtoMetadataColumnRecordDecorator(
                protoDataColumnRecordDecorator, maxComputeSinkConfig, maxComputeSchemaCache);
        protoMessageRecordConverter = new ProtoMessageRecordConverter(metadataColumnRecordDecorator, maxComputeSchemaCache);
    }

    @Test
    public void shouldConvertMessageToRecordWrapper() {
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        LocalDateTime expectedTimestampLocalDateTime = Instant.ofEpochMilli(
                123012311L).atZone(ZoneId.of("UTC"))
                .toLocalDateTime();
        LocalDateTime expectedPayloadLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                ZoneOffset.UTC
        );

        RecordWrappers recordWrappers = protoMessageRecordConverter.convert(Collections.singletonList(message));

        Assertions.assertThat(recordWrappers.getValidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getValidRecords().get(0);
        Assertions.assertThat(recordWrapper.getIndex()).isEqualTo(0);
        Assertions.assertThat(recordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Serializable[]{
                        expectedTimestampLocalDateTime,
                        "topic",
                        100L,
                        "id",
                        new ArrayList<>(Arrays.asList(
                                new SimpleStruct(
                                        TypeInfoFactory.getStructTypeInfo(
                                                Arrays.asList("name", "balance"),
                                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.FLOAT)
                                        ),
                                        Arrays.asList("name_1", 100.2f)
                                ),
                                new SimpleStruct(
                                        TypeInfoFactory.getStructTypeInfo(
                                                Arrays.asList("name", "balance"),
                                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.FLOAT)
                                        ),
                                        Arrays.asList("name_2", 50f)
                                )
                        )),
                        expectedPayloadLocalDateTime
                });
        Assertions.assertThat(recordWrapper.getErrorInfo()).isNull();
    }

    @Test
    public void shouldReturnRecordWrapperWithDeserializationErrorWhenIOExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        Mockito.doThrow(new IOException()).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        Assertions.assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        Assertions.assertThat(recordWrapper.getIndex()).isEqualTo(0);
        Assertions.assertThat(recordWrapper.getRecord())
                .isNull();
        Assertions.assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
    }

    @Test
    public void shouldReturnRecordWrapperWithUnknownFieldsErrorWhenUnknownFieldExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        com.google.protobuf.Message mockedMessage = getMockedMessage();
        Mockito.doThrow(new UnknownFieldsException(mockedMessage)).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        Assertions.assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        Assertions.assertThat(recordWrapper.getIndex()).isEqualTo(0);
        Assertions.assertThat(recordWrapper.getRecord())
                .isNull();
        Assertions.assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new UnknownFieldsException(mockedMessage), ErrorType.UNKNOWN_FIELDS_ERROR));
    }

    private static TestMaxComputeRecord.MaxComputeRecord getMockedMessage() {
        return TestMaxComputeRecord.MaxComputeRecord
                .newBuilder()
                .setId("id")
                .addAllInnerRecord(Arrays.asList(
                        TestMaxComputeRecord.InnerRecord.newBuilder()
                                .setName("name_1")
                                .setBalance(100.2f)
                                .build(),
                        TestMaxComputeRecord.InnerRecord.newBuilder()
                                .setName("name_2")
                                .setBalance(50f)
                                .build()
                ))
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(10002010)
                        .setNanos(1000)
                        .build())
                .build();
    }

}
