package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LocalDateTimeConverter implements Converter<LocalDateTime> {

    @Override
    public LocalDateTime convert(Method method, String s) {
        return LocalDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME);
    }

}
