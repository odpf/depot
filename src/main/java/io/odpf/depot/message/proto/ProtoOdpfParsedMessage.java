package io.odpf.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.UnknownFieldsException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.proto.converter.fields.DurationProtoField;
import io.odpf.depot.message.proto.converter.fields.MessageProtoField;
import io.odpf.depot.message.proto.converter.fields.ProtoField;
import io.odpf.depot.message.proto.converter.fields.ProtoFieldFactory;
import io.odpf.depot.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ProtoOdpfParsedMessage implements ParsedOdpfMessage {
    private final DynamicMessage dynamicMessage;

    private final Map<OdpfMessageSchema, Map<String, Object>> cachedMapping = new HashMap<>();

    public ProtoOdpfParsedMessage(DynamicMessage dynamicMessage) {
        this.dynamicMessage = dynamicMessage;
    }

    public String toString() {
        return dynamicMessage.toString();
    }

    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    @Override
    public void validate(OdpfSinkConfig config) {
        if (!config.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
            log.error("Unknown fields {}", UnknownProtoFields.toString(dynamicMessage.toByteArray()));
            throw new UnknownFieldsException(dynamicMessage);
        }
    }

    @Override
    public Map<String, Object> getMapping(OdpfMessageSchema schema) {
        if (schema.getSchema() == null) {
            throw new ConfigurationException("Schema is not configured");
        }
        return cachedMapping.computeIfAbsent(schema, x -> getMappings(dynamicMessage, (Properties) schema.getSchema()));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMappings(DynamicMessage message, Properties columnMapping) {
        if (message == null || columnMapping == null || columnMapping.isEmpty()) {
            return new HashMap<>();
        }
        Descriptors.Descriptor descriptorForType = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(columnMapping.size());
        columnMapping.forEach((key, value) -> {
            String columnName = value.toString();
            String columnIndex = key.toString();
            if (columnIndex.equals(Constants.Config.RECORD_NAME)) {
                return;
            }
            int protoIndex = Integer.parseInt(columnIndex);
            Descriptors.FieldDescriptor fieldDesc = descriptorForType.findFieldByNumber(protoIndex);
            if (fieldDesc != null && !message.getField(fieldDesc).toString().isEmpty()) {
                Object field = message.getField(fieldDesc);
                ProtoField protoField = ProtoFieldFactory.getField(fieldDesc, field);
                Object fieldValue = protoField.getValue();
                if (fieldValue instanceof List) {
                    addRepeatedFields(row, value, (List<Object>) fieldValue);
                    return;
                }
                if (fieldValue instanceof Instant) {
                    row.put(columnName, new DateTime(((Instant) fieldValue).toEpochMilli()));
                } else if (protoField.getClass().getName().equals(MessageProtoField.class.getName())
                        || protoField.getClass().getName().equals(DurationProtoField.class.getName())) {
                    Tuple<String, Object> nestedColumns = getNestedColumnName(field, value);
                    row.put(nestedColumns.getFirst(), nestedColumns.getSecond());
                } else {
                    row.put(columnName, fieldValue);
                }
            }
        });
        return row;
    }

    private Tuple<String, Object> getNestedColumnName(Object field, Object value) {
        try {
            String columnName = getNestedColumnName((Properties) value);
            Object fieldValue = getMappings((DynamicMessage) field, (Properties) value);
            return new Tuple<>(columnName, fieldValue);
        } catch (Exception e) {
            log.error("Exception::Handling nested field failure: {}", e.getMessage());
            throw e;
        }
    }

    private String getNestedColumnName(Properties value) {
        return value.get(Constants.Config.RECORD_NAME).toString();
    }

    private void addRepeatedFields(Map<String, Object> row, Object value, List<Object> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        String columnName = null;
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                assert value instanceof Properties;
                Properties nestedMappings = (Properties) value;
                repeatedNestedFields.add(getMappings((DynamicMessage) f, nestedMappings));
                columnName = getNestedColumnName(nestedMappings);
            } else {
                if (f instanceof Instant) {
                    repeatedNestedFields.add(new DateTime(((Instant) f).toEpochMilli()));
                } else {
                    repeatedNestedFields.add(f);
                }
                assert value instanceof String;
                columnName = (String) value;
            }
        }
        row.put(columnName, repeatedNestedFields);
    }


    public Object getFieldByName(String name, OdpfMessageSchema odpfMessageSchema) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        String[] keys = name.split("\\.");
        Object currentValue = dynamicMessage;
        Descriptors.FieldDescriptor descriptor = null;
        for (String key : keys) {
            if (!(currentValue instanceof DynamicMessage)) {
                throw new IllegalArgumentException("Invalid field config : " + name);
            }
            DynamicMessage message = (DynamicMessage) currentValue;
            descriptor = message.getDescriptorForType().findFieldByName(key);
            if (descriptor == null) {
                throw new IllegalArgumentException("Invalid field config : " + name);
            }
            currentValue = message.getField(descriptor);
        }
        return ProtoFieldFactory.getField(descriptor, currentValue);
    }
}
