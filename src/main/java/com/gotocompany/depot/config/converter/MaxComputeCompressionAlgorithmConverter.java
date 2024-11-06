package com.gotocompany.depot.config.converter;

import com.aliyun.odps.tunnel.io.CompressOption;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class MaxComputeCompressionAlgorithmConverter implements Converter<CompressOption.CompressAlgorithm> {

    @Override
    public CompressOption.CompressAlgorithm convert(Method method, String s) {
        return CompressOption.CompressAlgorithm.valueOf(s.toUpperCase());
    }

}
