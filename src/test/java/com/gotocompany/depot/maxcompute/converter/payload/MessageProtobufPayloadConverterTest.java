package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.type.DurationProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.MessageProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.StructProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.ProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class MessageProtobufPayloadConverterTest {

    private MessageProtobufPayloadConverter messagePayloadConverter;
    private Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestBuyerWrapper.getDescriptor();

    @Before
    public void init() {
        MessageProtobufTypeInfoConverter messageTypeInfoConverter = initializeTypeInfoConverters();
        List<ProtobufPayloadConverter> protobufPayloadConverters = initializePayloadConverter(messageTypeInfoConverter);
        messagePayloadConverter = new MessageProtobufPayloadConverter(messageTypeInfoConverter, protobufPayloadConverters);
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

        Object object = messagePayloadConverter.convert(new ProtoPayload(descriptor.getFields().get(0), wrapper.getField(descriptor.getFields().get(0)), true));

        assertThat(object)
                .extracting("typeInfo", "values")
                .containsExactly(expectedStructTypeInfo, expectedStructValues);
    }


    private MessageProtobufTypeInfoConverter initializeTypeInfoConverters() {
        List<ProtobufTypeInfoConverter> converters = new ArrayList<>();
        converters.add(new PrimitiveProtobufTypeInfoConverter());
        converters.add(new DurationProtobufTypeInfoConverter());
        converters.add(new StructProtobufTypeInfoConverter());
        converters.add(new TimestampProtobufTypeInfoConverter());
        MessageProtobufTypeInfoConverter messageTypeInfoConverter = new MessageProtobufTypeInfoConverter(converters);
        converters.add(messageTypeInfoConverter);
        return messageTypeInfoConverter;
    }

    private List<ProtobufPayloadConverter> initializePayloadConverter(MessageProtobufTypeInfoConverter messageTypeInfoConverter) {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        List<ProtobufPayloadConverter> protobufPayloadConverters = new ArrayList<>();
        protobufPayloadConverters.add(new DurationProtobufPayloadConverter(new DurationProtobufTypeInfoConverter()));
        protobufPayloadConverters.add(new PrimitiveProtobufPayloadConverter(new PrimitiveProtobufTypeInfoConverter()));
        protobufPayloadConverters.add(new StructProtobufPayloadConverter(new StructProtobufTypeInfoConverter()));
        protobufPayloadConverters.add(new TimestampProtobufPayloadConverter(new TimestampProtobufTypeInfoConverter(), maxComputeSinkConfig));
        protobufPayloadConverters.add(new MessageProtobufPayloadConverter(messageTypeInfoConverter, protobufPayloadConverters));
        return protobufPayloadConverters;
    }

}
