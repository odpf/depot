package io.odpf.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonOdpfParsedMessageTest {
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    @Test
    public void shouldGetEmptyMappingKeysForEmptyJsonObject() {
        //for empty json object
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(new JSONObject(), configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        assertEquals(Collections.emptyMap(), parsedMessageMapping);

    }

    @Test
    public void shouldGetEmptyMappingKeysForNullJsonObject() {
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(null, configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        assertEquals(Collections.emptyMap(), parsedMessageMapping);
    }

    @Test
    public void shouldGetMappings() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails, configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("first_name", "john doe");
        expectedMap.put("address", "planet earth");
        assertEquals(expectedMap, parsedMessageMapping);
    }

    @Test
    public void shouldReturnValueFromFlatJson() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails, configuration);
        Assert.assertEquals("john doe", parsedMessage.getFieldByName("first_name", null));
    }

    @Test
    public void shouldReturnValueFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails, configuration);
        Assert.assertEquals("david doe", parsedMessage.getFieldByName("family.brother", null));
    }

    @Test
    public void shouldThrowExceptionIfNotFound() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails, configuration);
        java.lang.IllegalArgumentException illegalArgumentException = Assert.assertThrows(java.lang.IllegalArgumentException.class, () -> parsedMessage.getFieldByName("family.sister", null));
        Assert.assertEquals("Invalid field config : family.sister", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldReturnListFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : [{\"brother\" : \"david doe\"}, {\"brother\" : \"cain doe\"}]"
                + "}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails, configuration);
        JSONArray family = (JSONArray) parsedMessage.getFieldByName("family", null);
        Assert.assertEquals(2, family.length());
        Assert.assertEquals("david doe", ((JSONObject) family.get(0)).get("brother"));
        Assert.assertEquals("cain doe", ((JSONObject) family.get(1)).get("brother"));
    }
}
