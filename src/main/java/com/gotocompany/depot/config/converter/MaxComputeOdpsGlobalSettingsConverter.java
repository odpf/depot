package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MaxComputeOdpsGlobalSettingsConverter implements Converter<Map<String, String>> {

    private static final String CONFIG_SEPARATOR = ",";
    private static final String KEY_VALUE_SEPARATOR = "=";

    @Override
    public Map<String, String> convert(Method method, String s) {
        if (Objects.isNull(s) || StringUtils.isEmpty(s.trim())) {
            return new HashMap<>();
        }
        String[] pairs = s.split(CONFIG_SEPARATOR);
        Map<String, String> settings = new HashMap<>();
        for (String pair : pairs) {
            String[] keyValue = pair.split(KEY_VALUE_SEPARATOR);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid key-value pair: " + pair);
            }
            settings.put(keyValue[0].trim(), keyValue[1].trim());
        }
        return settings;
    }

}
