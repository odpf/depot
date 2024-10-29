package com.gotocompany.depot.maxcompute.record;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSchemaCache maxComputeSchemaCache;
    private final MessageParser protoMessageParser;
    private final PartitioningStrategy partitioningStrategy;
    private final SinkConfig sinkConfig;

    public ProtoDataColumnRecordDecorator(RecordDecorator decorator,
                                          ConverterOrchestrator converterOrchestrator,
                                          MaxComputeSchemaCache maxComputeSchemaCache,
                                          MessageParser messageParser,
                                          SinkConfig sinkConfig,
                                          PartitioningStrategy partitioningStrategy) {
        super(decorator);
        this.converterOrchestrator = converterOrchestrator;
        this.maxComputeSchemaCache = maxComputeSchemaCache;
        this.protoMessageParser = messageParser;
        this.partitioningStrategy = partitioningStrategy;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public void append(RecordWrapper recordWrapper, Message message) throws IOException {
        String schemaClass = getSchemaClass();
        ParsedMessage parsedMessage = protoMessageParser.parse(message, sinkConfig.getSinkConnectorSchemaMessageMode(), schemaClass);
        parsedMessage.validate(sinkConfig);
        com.google.protobuf.Message protoMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        for (Descriptors.FieldDescriptor fieldDescriptor : protoMessage.getDescriptorForType().getFields()) {
            recordWrapper.getRecord()
                    .set(fieldDescriptor.getName(), converterOrchestrator.convert(fieldDescriptor, protoMessage.getField(fieldDescriptor)));
        }
        if (partitioningStrategy != null) {
            Object object = protoMessage.getField(protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName()));
            recordWrapper.setPartitionSpec(partitioningStrategy.getPartitionSpec(object));
        }
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
