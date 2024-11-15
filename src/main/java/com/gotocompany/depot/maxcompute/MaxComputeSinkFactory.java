package com.gotocompany.depot.maxcompute;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.converter.record.ProtoMessageRecordConverter;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecoratorFactory;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class MaxComputeSinkFactory {

    private final PartitioningStrategyFactory partitioningStrategyFactory;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final SinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final ConverterOrchestrator converterOrchestrator;
    private final StencilClient stencilClient;
    private final MaxComputeMetrics maxComputeMetrics;
    private MaxComputeClient maxComputeClient;
    private MaxComputeSchemaCache maxComputeSchemaCache;
    private PartitioningStrategy partitioningStrategy;
    private MessageParser messageParser;

    public MaxComputeSinkFactory(StatsDReporter statsDReporter,
                                 StencilClient stencilClient,
                                 Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.maxComputeSinkConfig = ConfigFactory.create(MaxComputeSinkConfig.class, env);
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
        this.converterOrchestrator = new ConverterOrchestrator();
        this.partitioningStrategyFactory = new PartitioningStrategyFactory(converterOrchestrator, maxComputeSinkConfig);
        this.stencilClient = stencilClient;
        this.maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        this.maxComputeClient = new MaxComputeClient(maxComputeSinkConfig, new Instrumentation(statsDReporter, MaxComputeClient.class), maxComputeMetrics);
    }

    public void init() {
        String schemaClass = SinkConnectorSchemaMessageMode.LOG_MESSAGE == sinkConfig.getSinkConnectorSchemaMessageMode() ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        Descriptors.Descriptor descriptor = stencilClient.get(schemaClass);
        this.partitioningStrategy = partitioningStrategyFactory.createPartitioningStrategy(descriptor);
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(converterOrchestrator, maxComputeSinkConfig, partitioningStrategy);
        this.maxComputeSchemaCache = new MaxComputeSchemaCache(maxComputeSchemaHelper, sinkConfig, converterOrchestrator, maxComputeClient);
        messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        this.maxComputeSchemaCache.setMessageParser(messageParser);
        this.maxComputeSchemaCache.updateSchema();
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

}
