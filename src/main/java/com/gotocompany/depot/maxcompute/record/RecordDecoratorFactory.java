package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.message.MessageParser;

public class RecordDecoratorFactory {

    public static RecordDecorator createRecordDecorator(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
            MaxComputeSchemaCache maxComputeSchemaCache,
            MessageParser messageParser,
            PartitioningStrategy partitioningStrategy,
            MaxComputeSinkConfig maxComputeSinkConfig,
            SinkConfig sinkConfig) {
        RecordDecorator dataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null, protobufConverterOrchestrator, messageParser, sinkConfig, partitioningStrategy);
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return dataColumnRecordDecorator;
        }
        return new ProtoMetadataColumnRecordDecorator(dataColumnRecordDecorator, maxComputeSinkConfig, maxComputeSchemaCache);
    }

}
