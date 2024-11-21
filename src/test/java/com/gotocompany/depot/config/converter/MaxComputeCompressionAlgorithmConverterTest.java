package com.gotocompany.depot.config.converter;

import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.exception.ConfigurationException;
import org.gradle.internal.impldep.org.testng.Assert;
import org.junit.Test;

public class MaxComputeCompressionAlgorithmConverterTest {

    @Test
    public void shouldParseToEnum() {
        String input = "ODPS_LZ4_FRAME";
        MaxComputeCompressionAlgorithmConverter converter = new MaxComputeCompressionAlgorithmConverter();

        CompressOption.CompressAlgorithm result = converter.convert(null, input);

        Assert.assertEquals(CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionGivenInvalidEnum() {
        String input = "NON_EXISTENT";
        MaxComputeCompressionAlgorithmConverter converter = new MaxComputeCompressionAlgorithmConverter();

        converter.convert(null, input);
    }

}
