package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.message.MessageParser;

public class MaxComputeSchemaCacheFactory {

    public static MaxComputeSchemaCache createMaxComputeSchemaCache(
            ConverterOrchestrator converterOrchestrator,
            MaxComputeSinkConfig maxComputeSinkConfig,
            PartitioningStrategy partitioningStrategy,
            SinkConfig sinkConfig,
            MaxComputeClient maxComputeClient,
            MessageParser messageParser
    ) {
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(new MaxComputeSchemaHelper(converterOrchestrator, maxComputeSinkConfig, partitioningStrategy), sinkConfig,
                converterOrchestrator, maxComputeClient);
        maxComputeSchemaCache.setMessageParser(messageParser);
        return maxComputeSchemaCache;
    }
}
