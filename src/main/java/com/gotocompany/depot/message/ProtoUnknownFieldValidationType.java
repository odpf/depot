package com.gotocompany.depot.message;

import com.google.protobuf.Message;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public enum ProtoUnknownFieldValidationType {
    MESSAGE {
        @Override
        public boolean shouldFilter(Object object) {
            return object instanceof Message;
        }

        @Override
        public Stream<Message> getMapper(Object object) {
            return Stream.of((Message) object);
        }
    },
    MESSAGE_ARRAY_FIRST_INDEX {
        @Override
        public boolean shouldFilter(Object object) {
            return isMessageOrMessageListType(object);
        }

        @Override
        public Stream<Message> getMapper(Object object) {
            if (object instanceof Message) {
                return Stream.of((Message) object);
            }
            if (object instanceof List) {
                return Optional.ofNullable(((List) object).get(0))
                        .filter(this::shouldFilter)
                        .map(o -> Stream.of((Message) o))
                        .orElseGet(Stream::empty);
            }
            return Stream.empty();
        }
    },
    MESSAGE_ARRAY_FULL {
        @Override
        public boolean shouldFilter(Object object) {
            return isMessageOrMessageListType(object);
        }

        @Override
        public Stream<Message> getMapper(Object object) {
            if (object instanceof Message) {
                return Stream.of((Message) object);
            }
            if (object instanceof List) {
                return Optional.of(object)
                        .filter(this::shouldFilter)
                        .map(messageList -> ((List<Message>) messageList).stream())
                        .orElseGet(Stream::empty);
            }
            return Stream.empty();
        }
    };

    public abstract boolean shouldFilter(Object object);
    public abstract Stream<Message> getMapper(Object object);

    private static boolean isMessageOrMessageListType(Object object) {
        if (object instanceof Message) {
            return true;
        }
        if (object instanceof List) {
            List<Object> list = (List) object;

            if (list.isEmpty()) {
                return false;
            }
            return list.get(0) instanceof Message;
        }
        return false;
    }
}
