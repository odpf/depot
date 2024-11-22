package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

public class SinkConfigUtils {

    public static String getProtoSchemaClassName(SinkConfig sinkConfig) {
        return SinkConnectorSchemaMessageMode.LOG_MESSAGE == sinkConfig.getSinkConnectorSchemaMessageMode()
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
