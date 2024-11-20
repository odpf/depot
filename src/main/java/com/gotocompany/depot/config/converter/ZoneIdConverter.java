package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.ZoneId;

public class ZoneIdConverter implements Converter<ZoneId> {

    @Override
    public ZoneId convert(Method method, String s) {
        return ZoneId.of(s);
    }

}
