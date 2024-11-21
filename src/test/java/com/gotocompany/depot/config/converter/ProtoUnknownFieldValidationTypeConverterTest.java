package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ProtoUnknownFieldValidationTypeConverterTest {

    private final ProtoUnknownFieldValidationTypeConverter protoUnknownFieldValidationTypeConverter = new ProtoUnknownFieldValidationTypeConverter();

    @Test
    public void shouldConvertToEnum() {
        String config = "MESSAGE";

        Assertions.assertEquals(ProtoUnknownFieldValidationType.MESSAGE,
                protoUnknownFieldValidationTypeConverter.convert(null, config));
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionGivenInvalidEnum() {
        String config = "INVALID";

        protoUnknownFieldValidationTypeConverter.convert(null, config);
    }

}
