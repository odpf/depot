package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TextMaxComputeTable;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Arrays;


public class MaxComputeSchemaHelperTest {

    private final Descriptors.Descriptor descriptor = TextMaxComputeTable.Table.getDescriptor();

    @Test
    public void shouldBuildPartitionedTableSchemaWithRootLevelMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        Mockito.when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);
        int expectedNonPartitionColumnCount = 7;
        int expectedPartitionColumnCount = 1;

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("__message_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("__kafka_topic", TypeInfoFactory.STRING),
                        Tuple.tuple("__kafka_offset", TypeInfoFactory.BIGINT)
                );
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    @Test
    public void shouldBuildPartitionedTableSchemaWithNestedMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("meta");
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        Mockito.when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        int expectedNonPartitionColumnCount = 5;
        int expectedPartitionColumnCount = 1;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig, partitioningStrategy);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("meta", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                Arrays.asList(TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)
                        ))
                );
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    @Test
    public void shouldBuildTableSchemaWithoutPartitionAndMeta() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        int expectedNonPartitionColumnCount = 4;
        int expectedPartitionColumnCount = 0;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.build(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ)
                );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenPartitionKeyIsNotFound() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("non_existent_partition_key");
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);

        maxComputeSchemaHelper.build(descriptor);
    }

}
