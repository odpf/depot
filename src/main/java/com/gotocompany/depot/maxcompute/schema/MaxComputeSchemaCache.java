package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Wrapper class that listens to schema updates and updates the MaxCompute table schema.
 * It also caches the MaxCompute schema.
 */
@Slf4j
public class MaxComputeSchemaCache extends DepotStencilUpdateListener {

    private final MaxComputeSchemaBuilder maxComputeSchemaBuilder;
    private final SinkConfig sinkConfig;
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MaxComputeClient maxComputeClient;
    private MaxComputeSchema maxComputeSchema;

    public MaxComputeSchemaCache(MaxComputeSchemaBuilder maxComputeSchemaBuilder,
                                 SinkConfig sinkConfig,
                                 ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                 MaxComputeClient maxComputeClient) {
        this.maxComputeSchemaBuilder = maxComputeSchemaBuilder;
        this.sinkConfig = sinkConfig;
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.maxComputeClient = maxComputeClient;
    }

    /**
     * Get the MaxCompute schema. This schema is single source of truth for MaxCompute table schema.
     * It is updated whenever the protobuf schema is updated.
     *
     * @return MaxComputeSchema
     */
    public MaxComputeSchema getMaxComputeSchema() {
        synchronized (this) {
            if (maxComputeSchema == null) {
                updateSchema();
            }
        }
        return maxComputeSchema;
    }

    /**
     * Update the MaxCompute table schema based on the new protobuf schema from stencil.
     *
     * @param newDescriptor new protobuf class descriptors
     */
    @Override
    public synchronized void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {
        Descriptors.Descriptor descriptor = newDescriptor.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    /**
     * Update the MaxCompute table schema based on the protobuf schema fetched from message parser.
     */
    @Override
    public synchronized void updateSchema() {
        Map<String, Descriptors.Descriptor> descriptorMap = ((ProtoMessageParser) getMessageParser()).getDescriptorMap();
        Descriptors.Descriptor descriptor = descriptorMap.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    private void updateMaxComputeTableSchema(Descriptors.Descriptor descriptor) {
        MaxComputeSchema localSchema = maxComputeSchemaBuilder.build(descriptor);
        protobufConverterOrchestrator.clearCache();
        try {
            maxComputeClient.createOrUpdateTable(localSchema.getTableSchema());
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
