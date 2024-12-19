package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;

public class RecordDecoratorFactory {

    /**
     * Create a record decorator based on the sink configuration.
     * Creates a nested decorator in case of metadata column being enabled.
     *
     * @param protobufConverterOrchestrator protobuf converter orchestrator to convert protobuf fields to maxcompute record
     * @param maxComputeSchemaCache maxcompute schema cache
     * @param messageParser message parser
     * @param partitioningStrategy partitioning strategy
     * @param maxComputeSinkConfig maxcompute sink configuration
     * @param sinkConfig sink configuration
     * @param statsDReporter statsd reporter
     * @param maxComputeMetrics maxcompute metrics
     * @return record decorator
     */
    public static RecordDecorator createRecordDecorator(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
            MaxComputeSchemaCache maxComputeSchemaCache,
            MessageParser messageParser,
            PartitioningStrategy partitioningStrategy,
            MaxComputeSinkConfig maxComputeSinkConfig,
            SinkConfig sinkConfig,
            StatsDReporter statsDReporter,
            MaxComputeMetrics maxComputeMetrics) {
        RecordDecorator dataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null,
                protobufConverterOrchestrator,
                messageParser,
                sinkConfig,
                partitioningStrategy,
                statsDReporter,
                maxComputeMetrics);
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return dataColumnRecordDecorator;
        }
        return new ProtoMetadataColumnRecordDecorator(dataColumnRecordDecorator, maxComputeSinkConfig, maxComputeSchemaCache);
    }

}
