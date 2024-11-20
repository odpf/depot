package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

public class ProtoMetadataColumnRecordDecoratorTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    private MaxComputeSinkConfig maxComputeSinkConfig;
    private MaxComputeSchemaCache maxComputeSchemaCache;
    private ProtoMetadataColumnRecordDecorator protoMetadataColumnRecordDecorator;

    @Before
    public void setup() {
        MaxComputeSinkConfig config = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(config.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(config.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(config.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        Mockito.when(config.getMetadataColumnsTypes()).thenReturn(Arrays.asList(
                new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        ));
        Mockito.when(config.getZoneId()).thenReturn(ZoneId.of("UTC"));
        initializeDecorator(config);
    }

    @Test
    public void shouldPopulateRecordWithNamespacedMetadata() throws IOException {
        Message message = new Message(
                null,
                new ProtoParsedMessage(getMockedMessage(), null),
                new Tuple<>("__message_timestamp", 10002010L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        Record record = new ArrayRecord(maxComputeSchemaCache.getMaxComputeSchema().getColumns());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        LocalDateTime expectedLocalDateTime = Instant.ofEpochMilli(10002010L)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();

        protoMetadataColumnRecordDecorator.decorate(recordWrapper, message);

        Assertions.assertThat(record.get(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()))
                .isEqualTo(new SimpleStruct(
                        TypeInfoFactory.getStructTypeInfo(Arrays.asList("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                Arrays.asList(TypeInfoFactory.TIMESTAMP, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)),
                        Arrays.asList(expectedLocalDateTime, "topic", 100L)
                ));
    }

    @Test
    public void shouldPopulateRecordWithNonNamespacedMetadata() throws IOException {
        MaxComputeSinkConfig mcSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(mcSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(mcSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(mcSinkConfig.getMetadataColumnsTypes()).thenReturn(Arrays.asList(
                new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        ));
        Mockito.when(mcSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        initializeDecorator(mcSinkConfig);
        Message message = new Message(
                null,
                new ProtoParsedMessage(getMockedMessage(), null),
                new Tuple<>("__message_timestamp", 10002010L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        Record record = new ArrayRecord(maxComputeSchemaCache.getMaxComputeSchema().getColumns());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        LocalDateTime expectedLocalDateTime = Instant.ofEpochMilli(10002010L)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();

        protoMetadataColumnRecordDecorator.decorate(recordWrapper, message);

        Assertions.assertThat(record)
                .satisfies(r -> {
                    Assertions.assertThat(r.get("__message_timestamp"))
                            .isEqualTo(expectedLocalDateTime);
                    Assertions.assertThat(r.get("__kafka_topic"))
                            .isEqualTo("topic");
                    Assertions.assertThat(r.get("__kafka_offset"))
                            .isEqualTo(100L);
                });
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

    private void initializeDecorator(MaxComputeSinkConfig sinkConfig) {
        this.maxComputeSinkConfig = sinkConfig;
        ConverterOrchestrator converterOrchestrator = new ConverterOrchestrator(sinkConfig);
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(converterOrchestrator, sinkConfig, null);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);
        maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        Mockito.when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        protoMetadataColumnRecordDecorator = new ProtoMetadataColumnRecordDecorator(null, sinkConfig, maxComputeSchemaCache);
    }
}
