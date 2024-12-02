package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;

public class MaxComputeSchemaCacheFactory {

    public static MaxComputeSchemaCache createMaxComputeSchemaCache(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
            MaxComputeSinkConfig maxComputeSinkConfig,
            PartitioningStrategy partitioningStrategy,
            SinkConfig sinkConfig,
            MaxComputeClient maxComputeClient
    ) {
        return new MaxComputeSchemaCache(new MaxComputeSchemaHelper(protobufConverterOrchestrator, maxComputeSinkConfig, partitioningStrategy), sinkConfig,
                protobufConverterOrchestrator, maxComputeClient);
    }
}
