package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ProtoUnknownFieldValidationTypeConverter implements Converter<ProtoUnknownFieldValidationType> {

    private static final String INVALID_ENUM_MESSAGE_FORMAT = "Invalid unknown field validation type: %s valid values are: %s";

    @Override
    public ProtoUnknownFieldValidationType convert(Method method, String s) {
        try {
            return ProtoUnknownFieldValidationType.valueOf(s);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format(INVALID_ENUM_MESSAGE_FORMAT, s,
                    Arrays.toString(ProtoUnknownFieldValidationType.values())), e);
        }
    }

}
