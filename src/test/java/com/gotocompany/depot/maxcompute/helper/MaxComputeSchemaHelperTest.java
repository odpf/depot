package com.gotocompany.depot.maxcompute.helper;

import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TextMaxComputeTable;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.sun.tools.javac.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import org.mockito.Mockito;


public class MaxComputeSchemaHelperTest {

    private final Descriptors.Descriptor descriptor = TextMaxComputeTable.Table.getDescriptor();

    @Test
    public void shouldBuildPartitionedTableSchemaWithRootLevelMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                List.of(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(
                new ConverterOrchestrator(), maxComputeSinkConfig
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ConverterOrchestrator(),
                maxComputeSinkConfig, partitioningStrategyFactory.createPartitioningStrategy(descriptor));
        int expectedNonPartitionColumnCount = 7;
        int expectedPartitionColumnCount = 1;

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "contacts"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        List.of("number"),
                                        List.of(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "name"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP),
                        Tuple.tuple("__message_timestamp", TypeInfoFactory.TIMESTAMP),
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
                List.of(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        int expectedNonPartitionColumnCount = 5;
        int expectedPartitionColumnCount = 1;
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(
                new ConverterOrchestrator(), maxComputeSinkConfig
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(
                new ConverterOrchestrator(), maxComputeSinkConfig, partitioningStrategyFactory.createPartitioningStrategy(descriptor));

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "contacts"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        List.of("number"),
                                        List.of(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "name"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP),
                        Tuple.tuple("meta", TypeInfoFactory.getStructTypeInfo(
                                List.of("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                List.of(TypeInfoFactory.TIMESTAMP, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)
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
        int expectedNonPartitionColumnCount = 4;
        int expectedPartitionColumnCount = 0;
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(
                new ConverterOrchestrator(), maxComputeSinkConfig
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ConverterOrchestrator(),
                maxComputeSinkConfig, partitioningStrategyFactory.createPartitioningStrategy(descriptor));

        MaxComputeSchema maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);

        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "contacts"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        List.of("number"),
                                        List.of(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "name"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP)
                );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenPartitionKeyIsNotFound() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                List.of(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("non_existent_partition_key");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(
                new ConverterOrchestrator(), maxComputeSinkConfig
        );
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(new ConverterOrchestrator(),
                maxComputeSinkConfig, partitioningStrategyFactory.createPartitioningStrategy(descriptor));

        maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);
    }

}
