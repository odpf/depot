package io.odpf.depot.message.json;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static io.odpf.depot.message.SinkConnectorSchemaMessageMode.LOG_KEY;
import static io.odpf.depot.message.SinkConnectorSchemaMessageMode.LOG_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class JsonOdpfMessageParserTest {

    private final OdpfSinkConfig defaultConfig = ConfigFactory.create(OdpfSinkConfig.class, Collections.emptyMap());
    private final Instrumentation instrumentation = mock(Instrumentation.class);
    private final JsonParserMetrics jsonParserMetrics = new JsonParserMetrics(defaultConfig);

    /*
            JSONObject.equals does reference check, so cant use assertEquals instead we use expectedJson.similar(actualJson)
            reference https://github.com/stleary/JSON-java/blob/master/src/test/java/org/json/junit/JSONObjectTest.java#L132
         */
    @Test
    public void shouldParseJsonLogMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, validJsonStr.getBytes());

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldPublishTimeTakenToCastJsonValuesToString() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, validJsonStr.getBytes());

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
        verify(instrumentation, times(1)).captureDurationSince(
                eq("application_sink_json_parse_operation_milliseconds"), any(Instant.class));
    }

    @Test
    public void shouldCastTheJSONValuesToString() throws IOException {
        Map<String, String> configMap = of("SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true");
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(config, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\n"
                + "  \"idfv\": \"FE533F4A-F776-4BEF-98B7-6BD1DFC2972C\",\n"
                + "  \"is_lat\": true,\n"
                + "  \"contributor_2_af_prt\": null,\n"
                + "  \"sdk_version\": 6.310932397154218,\n"
                + "  \"whole_number\": 2\n"
                + "}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, validJsonStr.getBytes());

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        String stringifiedJsonStr = "{\n"
                //normal string should remain as is
                + "  \"idfv\": \"FE533F4A-F776-4BEF-98B7-6BD1DFC2972C\",\n"

                //boolean should be converted to string
                + "  \"is_lat\": \"true\",\n"
                //null will not be there entirely
                //"  \"contributor_2_af_prt\": null,\n"

                //float should be converted to string
                + "  \"sdk_version\": \"6.310932397154218\",\n"

                //integer should be converted to string
                + "  \"whole_number\": \"2\"\n"
                + "}";
        JSONObject expectedJsonObject = new JSONObject(stringifiedJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowExceptionForNestedJsonNotSupported() {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String nestedJsonStr = "{\n"
                + "  \"event_value\": {\n"
                + "    \"CustomerLatitude\": \"-6.166895595817224\",\n"
                + "    \"fb_content_type\": \"product\"\n"
                + "  },\n"
                + "  \"ip\": \"210.210.175.250\",\n"
                + "  \"oaid\": null,\n"
                + "  \"event_time\": \"2022-05-06 08:03:43.561\",\n"
                + "  \"is_receipt_validated\": null,\n"
                + "  \"contributor_1_campaign\": null\n"
                + "}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, nestedJsonStr.getBytes());

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null));
        assertEquals("nested json structure not supported yet", exception.getMessage());

    }


    @Test
    public void shouldThrowErrorForInvalidLogMessage() {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, invalidJsonStr.getBytes());
        IOException ioException = assertThrows(IOException.class,
                () -> jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null));
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    @Test
    public void shouldThrowEmptyMessageException() {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, null);
        EmptyMessageException emptyMessageException = assertThrows(EmptyMessageException.class,
                () -> jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_MESSAGE, null));
        assertEquals("log message is empty", emptyMessageException.getMessage());
    }

    @Test
    public void shouldParseJsonKeyMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(validJsonStr.getBytes(), null);

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_KEY, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowErrorForInvalidKeyMessage() {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class,
                () -> jsonOdpfMessageParser.parse(jsonOdpfMessage, LOG_KEY, null));
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class,
                () -> jsonOdpfMessageParser.parse(jsonOdpfMessage, null, null));
        assertEquals("message mode not defined", ioException.getMessage());
    }
}
