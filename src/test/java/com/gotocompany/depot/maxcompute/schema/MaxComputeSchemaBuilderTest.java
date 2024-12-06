package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TextMaxComputeTable;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


public class MaxComputeSchemaBuilderTest {

    private final Descriptors.Descriptor descriptor = TextMaxComputeTable.Table.getDescriptor();

    @Test
    public void shouldBuildPartitionedTableSchemaWithRootLevelMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);
        int expectedNonPartitionColumnCount = 7;
        int expectedPartitionColumnCount = 1;

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
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
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    @Test
    public void shouldBuildPartitionedTableSchemaWithNestedMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("meta");
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        int expectedNonPartitionColumnCount = 5;
        int expectedPartitionColumnCount = 1;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig, partitioningStrategy);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
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
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    @Test
    public void shouldBuildTableSchemaWithoutPartitionAndMeta() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        int expectedNonPartitionColumnCount = 4;
        int expectedPartitionColumnCount = 0;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
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
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("non_existent_partition_key");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy);

        maxComputeSchemaBuilder.build(descriptor);
    }

}
