package com.gotocompany.depot.config.converter;

import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.exception.ConfigurationException;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;

public class MaxComputeCompressionAlgorithmConverter implements Converter<CompressOption.CompressAlgorithm> {

    private static final String INVALID_ENUM_MESSAGE_FORMAT = "Invalid compression algorithm: %s valid values are: %s";

    @Override
    public CompressOption.CompressAlgorithm convert(Method method, String s) {
        try {
            return CompressOption.CompressAlgorithm.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format(INVALID_ENUM_MESSAGE_FORMAT, s,
                    Arrays.toString(CompressOption.CompressAlgorithm.values())), e);
        }

    }

}
