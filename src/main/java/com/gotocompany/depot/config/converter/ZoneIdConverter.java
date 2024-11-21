package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.DateTimeException;
import java.time.ZoneId;

public class ZoneIdConverter implements Converter<ZoneId> {

    @Override
    public ZoneId convert(Method method, String s) {
        try {
            return ZoneId.of(s);
        } catch (Exception e) {
            throw new ConfigurationException("Invalid ZoneId: " + s, e);
        }
    }

}
