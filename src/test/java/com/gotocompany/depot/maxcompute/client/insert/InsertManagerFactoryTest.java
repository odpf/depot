package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class InsertManagerFactoryTest {

    @Test
    public void shouldCreatePartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class));

        Assertions.assertTrue(insertManager instanceof PartitionedInsertManager);
    }

    @Test
    public void shouldCreateNonPartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class));

        Assertions.assertTrue(insertManager instanceof NonPartitionedInsertManager);
    }

}
