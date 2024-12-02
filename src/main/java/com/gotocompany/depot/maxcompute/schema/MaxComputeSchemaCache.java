package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class MaxComputeSchemaCache extends DepotStencilUpdateListener {

    private final MaxComputeSchemaHelper maxComputeSchemaHelper;
    private final SinkConfig sinkConfig;
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MaxComputeClient maxComputeClient;
    private MaxComputeSchema maxComputeSchema;

    public MaxComputeSchemaCache(MaxComputeSchemaHelper maxComputeSchemaHelper,
                                 SinkConfig sinkConfig,
                                 ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                 MaxComputeClient maxComputeClient) {
        this.maxComputeSchemaHelper = maxComputeSchemaHelper;
        this.sinkConfig = sinkConfig;
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.maxComputeClient = maxComputeClient;
    }

    public MaxComputeSchema getMaxComputeSchema() {
        synchronized (this) {
            if (maxComputeSchema == null) {
                updateSchema();
            }
        }
        return maxComputeSchema;
    }

    @Override
    public synchronized void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {
        Descriptors.Descriptor descriptor = newDescriptor.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    @Override
    public synchronized void updateSchema() {
        Map<String, Descriptors.Descriptor> descriptorMap = ((ProtoMessageParser) getMessageParser()).getDescriptorMap();
        Descriptors.Descriptor descriptor = descriptorMap.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    private void updateMaxComputeTableSchema(Descriptors.Descriptor descriptor) {
        MaxComputeSchema localSchema = maxComputeSchemaHelper.build(descriptor);
        protobufConverterOrchestrator.clearCache();
        try {
            maxComputeClient.upsertTable(localSchema.getTableSchema());
            log.info("MaxCompute table upserted successfully");
            TableSchema serverSideTableSchema = maxComputeClient.getLatestTableSchema();
            maxComputeSchema = new MaxComputeSchema(
                    serverSideTableSchema,
                    localSchema.getMetadataColumns()
            );
        } catch (OdpsException e) {
            throw new MaxComputeTableOperationException("Error while updating MaxCompute table", e);
        }
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }
}
