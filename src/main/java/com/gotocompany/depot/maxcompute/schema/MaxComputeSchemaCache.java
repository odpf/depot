package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
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
    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeClient maxComputeClient;
    private MaxComputeSchema maxComputeSchema;

    public MaxComputeSchemaCache(MaxComputeSchemaHelper maxComputeSchemaHelper,
                                 SinkConfig sinkConfig,
                                 ConverterOrchestrator converterOrchestrator,
                                 MaxComputeClient maxComputeClient) {
        this.maxComputeSchemaHelper = maxComputeSchemaHelper;
        this.sinkConfig = sinkConfig;
        this.converterOrchestrator = converterOrchestrator;
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
    public void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {
        Descriptors.Descriptor descriptor;
        if (newDescriptor == null) {
            Map<String, Descriptors.Descriptor> descriptorMap = ((ProtoMessageParser) getMessageParser()).getDescriptorMap();
            descriptor = descriptorMap.get(getSchemaClass());
        } else {
            descriptor = newDescriptor.get(getSchemaClass());
        }
        maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(descriptor);
        converterOrchestrator.clearCache();
        try {
            maxComputeClient.upsertTable(maxComputeSchema.getTableSchema());
            log.info("MaxCompute table upserted successfully");
        } catch (OdpsException e) {
            throw new RuntimeException("Error while updating maxcompute table on callback", e);
        }
    }

    @Override
    public void updateSchema() {
        onSchemaUpdate(null);
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }
}
