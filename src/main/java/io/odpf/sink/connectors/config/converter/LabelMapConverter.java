package io.odpf.sink.connectors.config.converter;

import io.odpf.sink.connectors.common.Tuple;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LabelMapConverter implements Converter<Map<String, String>> {

    public Map<String, String> convert(Method method, String input) {
        List<Tuple<String, String>> listResult = ConverterUtils.convertToList(input);
        return listResult.stream().collect(Collectors.toMap(Tuple::getFirst, Tuple::getSecond));
    }
}

