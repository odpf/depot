package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class SinkConfigUtilsTest {

    @Test
    public void shouldReturnMessageClassWhenMessageModeIsLogMessage() {
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("messageClass");
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("keyClass");

        String result = SinkConfigUtils.getProtoSchemaClassName(sinkConfig);

        Assertions.assertEquals("messageClass", result);
    }

    @Test
    public void shouldReturnKeyClassWhenMessageModeIsLogKey() {
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_KEY);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("messageClass");
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("keyClass");

        String result = SinkConfigUtils.getProtoSchemaClassName(sinkConfig);

        Assertions.assertEquals("keyClass", result);
    }

}
