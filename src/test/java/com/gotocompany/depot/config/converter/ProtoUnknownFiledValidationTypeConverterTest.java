package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ProtoUnknownFiledValidationTypeConverterTest {

    private final ProtoUnknownFiledValidationTypeConverter protoUnknownFiledValidationTypeConverter = new ProtoUnknownFiledValidationTypeConverter();

    @Test
    public void shouldConvertToEnum() {
        String config = "MESSAGE";

        Assertions.assertEquals(ProtoUnknownFieldValidationType.MESSAGE,
                protoUnknownFiledValidationTypeConverter.convert(null, config));
    }

}
