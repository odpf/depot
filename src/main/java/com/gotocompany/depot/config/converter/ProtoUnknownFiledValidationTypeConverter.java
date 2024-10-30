package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ProtoUnknownFiledValidationTypeConverter implements Converter<ProtoUnknownFieldValidationType> {

    @Override
    public ProtoUnknownFieldValidationType convert(Method method, String s) {
        return ProtoUnknownFieldValidationType.valueOf(s);
    }

}
