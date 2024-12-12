package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class RecordDecoratorFactoryTest {

    @Test
    public void shouldCreateDataRecordDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.message.Message");

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                null, null, null, null,
                maxComputeSinkConfig, sinkConfig, Mockito.mock(Instrumentation.class), Mockito.mock(MaxComputeMetrics.class)
        );

        assertThat(recordDecorator)
                .isInstanceOf(ProtoDataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNull();
    }

    @Test
    public void shouldCreateDataRecordDecoratorWithNamespaceDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.message.Message");

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                null, null, null,
                null, maxComputeSinkConfig, sinkConfig, Mockito.mock(Instrumentation.class),
                Mockito.mock(MaxComputeMetrics.class)
        );

        assertThat(recordDecorator)
                .isInstanceOf(ProtoMetadataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNotNull()
                .isInstanceOf(ProtoDataColumnRecordDecorator.class);
    }
}
