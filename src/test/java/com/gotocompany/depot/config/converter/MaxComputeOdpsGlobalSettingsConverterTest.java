package com.gotocompany.depot.config.converter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

public class MaxComputeOdpsGlobalSettingsConverterTest {

    @Test
    public void shouldParseOdpsGlobalSettings() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "odps.schema.evolution.enable=true ,  odps.task.major.version=  sql_flighting_autopt ";

        Map<String, String> settings = converter.convert(null, odpsGlobalSettings);

        Assertions.assertEquals(2, settings.size());
        Assertions.assertTrue(settings.containsKey("odps.schema.evolution.enable"));
        Assertions.assertEquals("true", settings.get("odps.schema.evolution.enable"));
        Assertions.assertTrue(settings.containsKey("odps.task.major.version"));
        Assertions.assertEquals("sql_flighting_autopt", settings.get("odps.task.major.version"));
    }

    @Test
    public void shouldParseEmptyMapWhenGivenStringIsEmpty() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "         ";

        Map<String, String> settings = converter.convert(null, odpsGlobalSettings);

        Assertions.assertEquals(0, settings.size());
    }

    @Test
    public void shouldParseEmptyMapWhenGivenStringIsNull() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();

        Map<String, String> settings = converter.convert(null, null);

        Assertions.assertEquals(0, settings.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "odps.schema.evolution.enable+true ,  odps.task.major.version=sql_flighting_autopt";

        converter.convert(null, odpsGlobalSettings);
    }

}
