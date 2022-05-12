package io.odpf.depot.config.converter;

import io.odpf.depot.config.OdpfSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryHeadersConverterTest {
    @Test
    public void testConvertIfFetchHeadersValueEmpty() {
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "");
            }
        };
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, properties);
        Assert.assertEquals(0, config.getSchemaRegistryStencilFetchHeaders().size());
    }

    @Test
    public void shouldReturnZeroIfPropertyNotMentioned() {
        Map<String, String> properties = new HashMap<String, String>() {
        };
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, properties);
        Assert.assertEquals(0, config.getSchemaRegistryStencilFetchHeaders().size());
    }

    @Test
    public void shouldConvertHeaderKeyValuesWithHeaderObject() {
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "key1:value1 ,,, key2 : value2,");
            }
        };
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, properties);
        Assert.assertEquals((new BasicHeader("key1", "value1")).toString(), config.getSchemaRegistryStencilFetchHeaders().get(0).toString());
        Assert.assertEquals((new BasicHeader("key2", "value2")).toString(), config.getSchemaRegistryStencilFetchHeaders().get(1).toString());
        Assert.assertEquals(2, config.getSchemaRegistryStencilFetchHeaders().size());
    }
}
