package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.PartitionSpec;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.partition.DefaultPartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.TimestampPartitioningStrategy;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Decorator to convert protobuf message to maxcompute record.
 * Populates the data column and partition column.
 */
public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MessageParser protoMessageParser;
    private final PartitioningStrategy partitioningStrategy;
    private final SinkConfig sinkConfig;
    private final String partitionFieldName;
    private final boolean shouldReplaceOriginalColumn;
    private final String schemaClass;
    private final ProtoUnknownFieldValidationType protoUnknownFieldValidationType;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;
    private final boolean sinkConnectorSchemaProtoAllowUnknownFieldsEnable;
    private final boolean sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable;

    public ProtoDataColumnRecordDecorator(RecordDecorator decorator,
                                          ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                          MessageParser messageParser,
                                          SinkConfig sinkConfig,
                                          PartitioningStrategy partitioningStrategy,
                                          Instrumentation instrumentation,
                                          MaxComputeMetrics maxComputeMetrics) {
        super(decorator);
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.protoMessageParser = messageParser;
        this.partitioningStrategy = partitioningStrategy;
        this.sinkConfig = sinkConfig;
        this.partitionFieldName = Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::getOriginalPartitionColumnName)
                .orElse(null);
        this.shouldReplaceOriginalColumn = Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::shouldReplaceOriginalColumn)
                .orElse(false);
        this.schemaClass = sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        this.protoUnknownFieldValidationType = sinkConfig.getSinkConnectorSchemaProtoUnknownFieldsValidation();
        this.instrumentation = instrumentation;
        this.maxComputeMetrics = maxComputeMetrics;
        this.sinkConnectorSchemaProtoAllowUnknownFieldsEnable = sinkConfig.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();
        this.sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable = sinkConfig.getSinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable();
    }

    /**
     * Converts protobuf message to maxcompute record, populating the data column and partition column.
     *
     * @param recordWrapper record template to be populated
     * @param message protobuf raw message
     * @return populated record
     * @throws IOException if an error occurs while processing the message
     */
    @Override
    public RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException {
        ParsedMessage parsedMessage = protoMessageParser.parse(message, sinkConfig.getSinkConnectorSchemaMessageMode(), schemaClass);
        if (!sinkConnectorSchemaProtoAllowUnknownFieldsEnable) {
            Instant unknownFieldValidationStart = Instant.now();
            parsedMessage.validate(protoUnknownFieldValidationType);
            if (sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable) {
                instrumentation.captureDurationSince(
                        maxComputeMetrics.getMaxComputeUnknownFieldValidationLatencyMetric(),
                        unknownFieldValidationStart,
                        String.format(MaxComputeMetrics.MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG, protoUnknownFieldValidationType)
                );
            }
        }
        com.google.protobuf.Message protoMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        Map<Descriptors.FieldDescriptor, Object> fields = protoMessage.getAllFields();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
            if (entry.getKey().getName().equals(partitionFieldName) && shouldReplaceOriginalColumn) {
                continue;
            }
            recordWrapper.getRecord()
                    .set(entry.getKey().getName(), protobufConverterOrchestrator.toMaxComputeValue(entry.getKey(), entry.getValue()));
        }
        PartitionSpec partitionSpec = null;
        if (partitioningStrategy != null && partitioningStrategy instanceof DefaultPartitioningStrategy) {
            Descriptors.FieldDescriptor partitionFieldDescriptor = protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName());
            Object object = protoMessage.hasField(partitionFieldDescriptor) ? protoMessage.getField(protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName())) : null;
            partitionSpec = partitioningStrategy.getPartitionSpec(object);
        }
        if (partitioningStrategy != null && partitioningStrategy instanceof TimestampPartitioningStrategy) {
            partitionSpec = partitioningStrategy.getPartitionSpec(recordWrapper.getRecord());
        }
        return new RecordWrapper(recordWrapper.getRecord(), recordWrapper.getIndex(), recordWrapper.getErrorInfo(), partitionSpec);
    }

}
