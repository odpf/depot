package com.gotocompany.depot.message;

import com.google.protobuf.Message;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtoUnknownFieldValidationTypeTest {

    @Test
    public void shouldFilterMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Message message = Mockito.mock(Message.class);

        Assertions.assertTrue(type.shouldFilter(message));
    }

    @Test
    public void shouldNotFilterNonMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Integer message = 2;

        Assertions.assertFalse(type.shouldFilter(message));
    }

    @Test
    public void shouldReturnStreamOfMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Message message = Mockito.mock(Message.class);

        Assertions.assertEquals(type.getMapper(message).collect(Collectors.toList()),
                Stream.of(message).collect(Collectors.toList()));
    }

    @Test
    public void shouldFilterMessageOrMessageListType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message = Mockito.mock(com.google.protobuf.Message.class);
        List<Message> messageList = Collections.singletonList(message);

        Assertions.assertTrue(type.shouldFilter(message));
        Assertions.assertTrue(type.shouldFilter(messageList));
    }

    @Test
    public void shouldFilterOutNonMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Integer message = 2;

        Assertions.assertFalse(type.shouldFilter(message));
    }

    @Test
    public void shouldFilterOutEmptyMessageList() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        List<Message> messageList = new ArrayList<>();

        Assertions.assertFalse(type.shouldFilter(messageList));
    }

    @Test
    public void shouldFilterOutNullObject() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;

        Assertions.assertFalse(type.shouldFilter(null));
    }

    @Test
    public void shouldMapToSingularStreamOfMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message = Mockito.mock(Message.class);

        List<Message> result = type.getMapper(message).collect(Collectors.toList());

        Assertions.assertEquals(Stream.of(message).collect(Collectors.toList()), result);
    }

    @Test
    public void shouldMapToStreamOfMessagesOnlyFirstIndex() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        List<Message> result = type.getMapper(objects).collect(Collectors.toList());

        Assertions.assertEquals(Stream.of(message1).collect(Collectors.toList()), result);
    }

    @Test
    public void shouldMapToEmptyStreamWhenNonMessageListIsGiven() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Integer message = 2;

        List<Message> result = type.getMapper(message).collect(Collectors.toList());

        Assertions.assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void shouldFilterFullArrayMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FULL;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        Assertions.assertTrue(type.shouldFilter(objects));
    }

    @Test
    public void shouldMapFullArrayToMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FULL;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        List<Message> result = type.getMapper(objects).collect(Collectors.toList());

        Assertions.assertEquals(objects, result);
    }
}
