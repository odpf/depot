package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestMessage;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.message.proto.ProtoOdpfParsedMessage;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.stencil.Parser;
import io.odpf.stencil.StencilClientFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TemplateTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private ParsedOdpfMessage parsedTestMessage;
    private ParsedOdpfMessage parsedBookingMessage;
    private OdpfMessageSchema schemaTest;
    private OdpfMessageSchema schemaBooking;

    @Before
    public void setUp() throws Exception {
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        OdpfMessage message = new OdpfMessage(testKey.toByteArray(), testMessage.toByteArray());
        OdpfMessage bookingMessage = new OdpfMessage(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
        Parser protoParserTest = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        parsedTestMessage = new ProtoOdpfParsedMessage(protoParserTest.parse((byte[]) message.getLogMessage()));
        Parser protoParserBooking = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        parsedBookingMessage = new ProtoOdpfParsedMessage(protoParserBooking.parse((byte[]) bookingMessage.getLogMessage()));
        when(redisSinkConfig.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        ProtoOdpfMessageParser messageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(redisSinkConfig, statsDReporter);
        schemaTest = messageParser.getSchema("io.odpf.depot.TestMessage", descriptorsMap);
        schemaBooking = messageParser.getSchema("io.odpf.depot.TestBookingLogMessage", descriptorsMap);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() {
        Template template = new Template("Test-%s,order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage, schemaTest));
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() {
        Template template = new Template("Test-%s, order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage, schemaTest));
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() {
        Template template = new Template("Test-%.2f,amount_paid_by_cash");
        assertEquals("Test-12.30", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() {
        Template template = new Template("Test-%d,customer_total_fare_without_surge");
        assertEquals("Test-2000", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new Template(null));
        assertEquals("Template 'null' is invalid", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new Template(""));
        assertEquals("Template '' is invalid", e.getMessage());
    }

    @Test
    public void shouldAcceptStringForCollectionKey() {
        Template template = new Template("Test");
        assertEquals("Test", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldNotAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new Template("Test-%s%d%b,t1,t2"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=3, values=2", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> new Template("Test-%s%s%y,order_number,order_details"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=2, values=2", e.getMessage());
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithMultipleVariables() {
        Template template = new Template("Test-%s::%s, order_number, order_details");
        assertEquals("Test-test-order::ORDER-DETAILS", template.parse(parsedTestMessage, schemaTest));
    }
}
