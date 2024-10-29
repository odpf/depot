package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;

public class ProtoDataColumnRecordDecoratorTest {

    private static final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    private MaxComputeSchemaHelper maxComputeSchemaHelper;
    private ProtoDataColumnRecordDecorator protoDataColumnRecordDecorator;

    @Before
    public void setup() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);

        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig);
    }

    @Test
    public void decorateShouldAppendDataColumnToRecord() throws IOException {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        java.sql.Timestamp expectedTimestamp = new java.sql.Timestamp(10002010L * 1000);
        expectedTimestamp.setNanos(1000);
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) maxComputeSchema.getDataColumns().get("inner_record")).getElementTypeInfo();
        protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        Assertions.assertThat(record)
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f))
                        ),
                        expectedTimestamp});
    }

    private void instantiateProtoDataColumnRecordDecorator(SinkConfig sinkConfig, MaxComputeSinkConfig maxComputeSinkConfig) throws IOException {
        ConverterOrchestrator converterOrchestrator = new ConverterOrchestrator();
        maxComputeSchemaHelper = new MaxComputeSchemaHelper(
                converterOrchestrator,
                maxComputeSinkConfig,
                null
        );

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(DESCRIPTOR);
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        Mockito.when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);

        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        ParsedMessage parsedMessage = Mockito.mock(ParsedMessage.class);
        Mockito.when(parsedMessage.getRaw()).thenReturn(getMockedMessage());
        Mockito.when(protoMessageParser.parse(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(parsedMessage);

        protoDataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(
                null,
                converterOrchestrator,
                protoMessageParser,
                sinkConfig,
                null
        );
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
