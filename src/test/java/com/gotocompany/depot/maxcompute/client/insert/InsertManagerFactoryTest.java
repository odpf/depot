package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class InsertManagerFactoryTest {

    @Test
    public void shouldCreatePartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class), Mockito.mock(Instrumentation.class), Mockito.mock(MaxComputeMetrics.class));

        assertTrue(insertManager instanceof PartitionedInsertManager);
    }

    @Test
    public void shouldCreateNonPartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class), Mockito.mock(Instrumentation.class), Mockito.mock(MaxComputeMetrics.class));

        assertTrue(insertManager instanceof NonPartitionedInsertManager);
    }

}
