package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaxComputeSchemaCacheTest {

    @Test
    public void shouldBuildAndReturnMaxComputeSchema() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaHelper.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        when(sinkConfig.getSinkConnectorSchemaProtoKeyClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        TableSchema finalMockedTableSchema = Mockito.mock(TableSchema.class);
        Mockito.doReturn(finalMockedTableSchema)
                .when(maxComputeClient)
                .getLatestTableSchema();

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        verify(maxComputeClient, Mockito.times(1))
                .createOrUpdateTable(Mockito.any());
        assertEquals(finalMockedTableSchema, maxComputeSchema.getTableSchema());
    }

    @Test
    public void shouldReturnMaxComputeSchemaIfExists() throws OdpsException, NoSuchFieldException, IllegalAccessException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaHelper.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        Field field = MaxComputeSchemaCache.class.getDeclaredField("maxComputeSchema");
        field.setAccessible(true);
        field.set(maxComputeSchemaCache, mockedMaxComputeSchema);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        verify(maxComputeClient, Mockito.times(0))
                .createOrUpdateTable(Mockito.any());
        assertEquals(mockedMaxComputeSchema, maxComputeSchema);
    }

    @Test
    public void shouldUpdateSchemaBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaHelper.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        Mockito.doReturn(Mockito.mock(TableSchema.class))
                .when(maxComputeClient)
                .getLatestTableSchema();

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        verify(maxComputeClient, Mockito.times(2))
                .createOrUpdateTable(Mockito.any());
    }

    @Test
    public void shouldUpdateSchemaUsingLogKeyBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaHelper.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_KEY);
        when(sinkConfig.getSinkConnectorSchemaProtoKeyClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        Mockito.doReturn(Mockito.mock(TableSchema.class))
                .when(maxComputeClient)
                .getLatestTableSchema();

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        verify(maxComputeClient, Mockito.times(2))
                .createOrUpdateTable(Mockito.any());
    }

    @Test(expected = MaxComputeTableOperationException.class)
    public void shouldThrowMaxComputeTableOperationExceptionWhenUpsertIsFailing() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaHelper maxComputeSchemaHelper = Mockito.mock(MaxComputeSchemaHelper.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaHelper.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaHelper,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doThrow(new OdpsException("Invalid schema"))
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);
    }

}
