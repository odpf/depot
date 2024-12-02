package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.DefaultPartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.TimestampPartitioningStrategy;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProtoDataColumnRecordDecoratorTest {

    private static final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    private MaxComputeSchemaHelper maxComputeSchemaHelper;
    private ProtoDataColumnRecordDecorator protoDataColumnRecordDecorator;

    @Before
    public void setup() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, null, getMockedMessage());
    }

    @Test
    public void decorateShouldProcessDataColumnToRecord() throws IOException {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedLocalDateTime});
    }

    @Test
    public void decorateShouldProcessDataColumnToRecordAndOmitPartitionColumnIfPartitionedByPrimitiveTypes() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("id");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("id");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                maxComputeSinkConfig);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedLocalDateTime});
    }

    @Test
    public void decorateShouldProcessDataColumnToRecordAndShouldNotOmitOriginalColumnIfPartitionedByTimestamp() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_key");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new TimestampPartitioningStrategy(maxComputeSinkConfig);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        "id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedLocalDateTime});
    }

    @Test
    public void decorateShouldSetDefaultPartitioningSpecWhenProtoFieldNotExists() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_key");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new TimestampPartitioningStrategy(maxComputeSinkConfig);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = TestMaxComputeRecord.MaxComputeRecord
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
                .build();
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, maxComputeRecord);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        Message message = new Message(null, maxComputeRecord.toByteArray());
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedWrapper =
                protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        "id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        null});
        assertThat(decoratedWrapper.getPartitionSpec().toString())
                .isEqualTo("__partition_key='__NULL__'");
    }

    @Test
    public void decorateShouldPutDefaultPartitionSpec() throws IOException {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedLocalDateTime});
    }

    @Test
    public void decorateShouldCallInjectedDecorator() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        when(recordDecorator.decorate(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, recordDecorator, null, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC);
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecord = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecord.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedLocalDateTime});
        verify(recordDecorator, Mockito.times(1))
                .decorate(Mockito.any(), Mockito.any());
    }

    private void instantiateProtoDataColumnRecordDecorator(SinkConfig sinkConfig, MaxComputeSinkConfig maxComputeSinkConfig,
                                                           RecordDecorator recordDecorator,
                                                           PartitioningStrategy partitioningStrategy,
                                                           com.google.protobuf.Message mockedMessage) throws IOException {
        ProtobufConverterOrchestrator protobufConverterOrchestrator = new ProtobufConverterOrchestrator(maxComputeSinkConfig);
        maxComputeSchemaHelper = new MaxComputeSchemaHelper(
                protobufConverterOrchestrator,
                maxComputeSinkConfig,
                partitioningStrategy
        );
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(DESCRIPTOR);
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        ParsedMessage parsedMessage = Mockito.mock(ParsedMessage.class);
        when(parsedMessage.getRaw()).thenReturn(mockedMessage);
        when(protoMessageParser.parse(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(parsedMessage);
        protoDataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(
                recordDecorator,
                protobufConverterOrchestrator,
                protoMessageParser,
                sinkConfig,
                partitioningStrategy
        );
    }

    private static TypeInfo getDataColumnTypeByName(TableSchema tableSchema, String columnName) {
        return tableSchema.getColumns()
                .stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst()
                .map(com.aliyun.odps.Column::getTypeInfo)
                .orElse(null);
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
