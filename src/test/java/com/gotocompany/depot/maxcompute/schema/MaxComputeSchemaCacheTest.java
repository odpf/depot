package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class MaxComputeSchemaCacheTest {

    @Test
    public void shouldBuildAndReturnMaxComputeSchema() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        Mockito.when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(
                null,
                null,
                null,
                null
        );
        Mockito.when(maxComputeSchemaHelper.buildMaxComputeSchema(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ConverterOrchestrator(),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .upsertTable(Mockito.any());

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        Mockito.verify(maxComputeClient, Mockito.times(1))
                .upsertTable(Mockito.any());
        Assertions.assertEquals(mockedMaxComputeSchema, maxComputeSchema);
    }

    @Test
    public void shouldReturnMaxComputeSchemaIfExists() throws OdpsException, NoSuchFieldException, IllegalAccessException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        Mockito.when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(
                null,
                null,
                null,
                null
        );
        Mockito.when(maxComputeSchemaHelper.buildMaxComputeSchema(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ConverterOrchestrator(),
                maxComputeClient
        );
        Field field = MaxComputeSchemaCache.class.getDeclaredField("maxComputeSchema");
        field.setAccessible(true);
        field.set(maxComputeSchemaCache, mockedMaxComputeSchema);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        Mockito.verify(maxComputeClient, Mockito.times(0))
                .upsertTable(Mockito.any());
        Assertions.assertEquals(mockedMaxComputeSchema, maxComputeSchema);
    }

    @Test
    public void shouldUpdateSchemaBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(
                null,
                null,
                null,
                null
        );
        Mockito.when(maxComputeSchemaHelper.buildMaxComputeSchema(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ConverterOrchestrator(),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .upsertTable(Mockito.any());

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        Mockito.verify(maxComputeClient, Mockito.times(2))
                .upsertTable(Mockito.any());
    }

    @Test
    public void shouldUpdateSchemaUsingLogKeyBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(
                null,
                null,
                null,
                null
        );
        Mockito.when(maxComputeSchemaHelper.buildMaxComputeSchema(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_KEY);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoKeyClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ConverterOrchestrator(),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .upsertTable(Mockito.any());

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        Mockito.verify(maxComputeClient, Mockito.times(2))
                .upsertTable(Mockito.any());
    }

    @Test(expected = MaxComputeTableOperationException.class)
    public void shouldThrowMaxComputeTableOperationExceptionWhenUpsertIsFailing() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(
                null,
                null,
                null,
                null
        );
        Mockito.when(maxComputeSchemaHelper.buildMaxComputeSchema(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        Mockito.when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        Mockito.when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ConverterOrchestrator(),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doThrow(new OdpsException("Invalid schema"))
                .when(maxComputeClient)
                .upsertTable(Mockito.any());

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);
    }
}
