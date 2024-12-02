package com.gotocompany.depot.config.converter;

import org.junit.Test;

import java.time.DateTimeException;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class LocalDateTimeConverterTest {

    private final LocalDateTimeConverter localDateTimeConverter = new LocalDateTimeConverter();

    @Test
    public void shouldConvertToLocalDateTime() {
        String input = "2024-01-01T00:00:00";
        LocalDateTime expected = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

        LocalDateTime localDateTime = localDateTimeConverter.convert(null, input);

        assertEquals(expected, localDateTime);
    }

    @Test(expected = DateTimeException.class)
    public void shouldThrowExceptionWhenInputIsInvalid() {
        String input = "12-312024-01-01T00:00:00Z";
        localDateTimeConverter.convert(null, input);
    }

}
