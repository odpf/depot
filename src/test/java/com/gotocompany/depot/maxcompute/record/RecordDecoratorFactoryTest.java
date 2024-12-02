package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class RecordDecoratorFactoryTest {

    @Test
    public void shouldCreateDataRecordDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                null, null, null, null, maxComputeSinkConfig, null
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

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                null, null, null, null, maxComputeSinkConfig, null
        );
        assertThat(recordDecorator)
                .isInstanceOf(ProtoMetadataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNotNull()
                .isInstanceOf(ProtoDataColumnRecordDecorator.class);
    }
}
