package com.gotocompany.depot.maxcompute;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.converter.record.ProtoMessageRecordConverter;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecoratorFactory;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCacheFactory;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.SinkConfigUtils;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;
import java.util.Optional;

public class MaxComputeSinkFactory {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final SinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeMetrics maxComputeMetrics;
    private final MaxComputeClient maxComputeClient;

    private MaxComputeSchemaCache maxComputeSchemaCache;
    private PartitioningStrategy partitioningStrategy;
    private MessageParser messageParser;

    public MaxComputeSinkFactory(StatsDReporter statsDReporter,
                                 StencilClient stencilClient,
                                 Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.maxComputeSinkConfig = ConfigFactory.create(MaxComputeSinkConfig.class, env);
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
        this.stencilClient = stencilClient;
        this.converterOrchestrator = new ConverterOrchestrator(maxComputeSinkConfig);
        this.maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        this.maxComputeClient = new MaxComputeClient(maxComputeSinkConfig, new Instrumentation(statsDReporter, MaxComputeClient.class), maxComputeMetrics);
    }

    public void init() {
        validateConfig();
        Descriptors.Descriptor descriptor = stencilClient.get(SinkConfigUtils.getProtoSchemaClassName(sinkConfig));
        this.partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(converterOrchestrator, maxComputeSinkConfig, descriptor);
        this.maxComputeSchemaCache = MaxComputeSchemaCacheFactory.createMaxComputeSchemaCache(converterOrchestrator,
                maxComputeSinkConfig, partitioningStrategy, sinkConfig, maxComputeClient);
        this.messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        maxComputeSchemaCache.setMessageParser(messageParser);
        Optional.ofNullable(this.partitioningStrategy)
                .ifPresent(ps -> ps.setMaxComputeSchemaCache(maxComputeSchemaCache));
        maxComputeSchemaCache.updateSchema();
    }

    public Sink create() {
        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                converterOrchestrator,
                maxComputeSchemaCache,
                messageParser,
                partitioningStrategy,
                maxComputeSinkConfig,
                sinkConfig
        );
        ProtoMessageRecordConverter protoMessageRecordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        return new MaxComputeSink(maxComputeClient, protoMessageRecordConverter,
                new Instrumentation(statsDReporter, MaxComputeSink.class), maxComputeMetrics);
    }

    private void validateConfig() {
        this.maxComputeSinkConfig.getMaxComputeCompressionAlgorithm();
        this.maxComputeSinkConfig.getZoneId();
    }

}
