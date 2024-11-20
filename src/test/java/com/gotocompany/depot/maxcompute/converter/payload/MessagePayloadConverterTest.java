package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.type.DurationTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.StructTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TypeInfoConverter;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessagePayloadConverterTest {

    private MessagePayloadConverter messagePayloadConverter;
    private Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestBuyerWrapper.getDescriptor();

    @Before
    public void init() {
        MessageTypeInfoConverter messageTypeInfoConverter = initializeTypeInfoConverters();
        List<PayloadConverter> payloadConverters = initializePayloadConverter(messageTypeInfoConverter);
        messagePayloadConverter = new MessagePayloadConverter(messageTypeInfoConverter, payloadConverters);
    }

    @Test
    public void shouldConvertToStruct() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(1704067200)
                .setNanos(0)
                .build();
        Duration duration = Duration.newBuilder()
                .setSeconds(100)
                .build();
        TestMaxComputeTypeInfo.TestBuyer message = TestMaxComputeTypeInfo.TestBuyer.newBuilder()
                .setName("buyerName")
                .setCart(TestMaxComputeTypeInfo.TestCart.newBuilder()
                        .setCartId("cart_id")
                        .addAllItems(Arrays.asList(
                                TestMaxComputeTypeInfo.TestItem.newBuilder()
                                        .setId("item1")
                                        .setQuantity(1)
                                        .build(),
                                TestMaxComputeTypeInfo.TestItem.newBuilder()
                                        .setId("item2")
                                        .build()))
                        .setCreatedAt(timestamp)
                        .setCartAge(duration)
                )
                .setCreatedAt(timestamp)
                .build();
        TestMaxComputeTypeInfo.TestBuyerWrapper wrapper = TestMaxComputeTypeInfo.TestBuyerWrapper
                .newBuilder()
                .setBuyer(message)
                .build();
        StructTypeInfo durationTypeInfo = TypeInfoFactory.getStructTypeInfo(Arrays.asList("seconds", "nanos"), Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT));
        StructTypeInfo itemTypeInfo = TypeInfoFactory.getStructTypeInfo(Arrays.asList("id", "quantity"), Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.INT));
        StructTypeInfo cartTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("cart_id", "items", "created_at", "cart_age"),
                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(itemTypeInfo), TypeInfoFactory.TIMESTAMP_NTZ, durationTypeInfo)
        );
        StructTypeInfo expectedStructTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("name", "cart", "created_at"),
                Arrays.asList(TypeInfoFactory.STRING, cartTypeInfo, TypeInfoFactory.TIMESTAMP_NTZ)
        );
        List<Object> expectedStructValues = Arrays.asList(
                "buyerName",
                new SimpleStruct(cartTypeInfo,
                        Arrays.asList(
                                "cart_id",
                                Arrays.asList(new SimpleStruct(itemTypeInfo, Arrays.asList("item1", 1)), new SimpleStruct(itemTypeInfo, Arrays.asList("item2", null))),
                                LocalDateTime.ofEpochSecond(timestamp.getSeconds(), 0, java.time.ZoneOffset.UTC),
                                new SimpleStruct(durationTypeInfo, Arrays.asList(duration.getSeconds(), duration.getNanos())))),
                LocalDateTime.ofEpochSecond(timestamp.getSeconds(), 0, java.time.ZoneOffset.UTC)
        );

        Object object = messagePayloadConverter.convert(descriptor.getFields().get(0), wrapper.getField(descriptor.getFields().get(0)));

        Assertions.assertThat(object)
                .extracting("typeInfo", "values")
                .containsExactly(expectedStructTypeInfo, expectedStructValues);
    }


    private MessageTypeInfoConverter initializeTypeInfoConverters() {
        List<TypeInfoConverter> converters = new ArrayList<>();
        converters.add(new PrimitiveTypeInfoConverter());
        converters.add(new DurationTypeInfoConverter());
        converters.add(new StructTypeInfoConverter());
        converters.add(new TimestampTypeInfoConverter());
        MessageTypeInfoConverter messageTypeInfoConverter = new MessageTypeInfoConverter(converters);
        converters.add(messageTypeInfoConverter);
        return messageTypeInfoConverter;
    }

    private List<PayloadConverter> initializePayloadConverter(MessageTypeInfoConverter messageTypeInfoConverter) {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        List<PayloadConverter> payloadConverters = new ArrayList<>();
        payloadConverters.add(new DurationPayloadConverter(new DurationTypeInfoConverter()));
        payloadConverters.add(new PrimitivePayloadConverter(new PrimitiveTypeInfoConverter()));
        payloadConverters.add(new StructPayloadConverter(new StructTypeInfoConverter()));
        payloadConverters.add(new TimestampPayloadConverter(new TimestampTypeInfoConverter(), maxComputeSinkConfig));
        payloadConverters.add(new MessagePayloadConverter(messageTypeInfoConverter, payloadConverters));
        return payloadConverters;
    }

}
