# Generic

All sinks require the following variables to be set

## `SINK_METRICS_APPLICATION_PREFIX`

Application prefix for sink metrics.

* Example value: `application_`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS`

Message log-message schema class

* Example value: `com.gotocompany.schema.MessageClass`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS`

Message log-key schema class

* Example value: `com.gotocompany.schema.KeyClass`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_DATA_TYPE`

Message raw data type

* Example value: `JSON`
* Type: `required`
* Default: `PROTOBUF`

## `SINK_CONNECTOR_SCHEMA_MESSAGE_MODE`

The type of raw message to read from

* Example value: `LOG_MESSAGE`
* Type: `required`
* Default: `LOG_MESSAGE`

## `SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE`

Allow unknown fields in proto schema

* Example value: `true`
* Type: `required`
* Default: `false`

## `SINK_CONNECTOR_SCHEMA_PROTO_UNKNOWN_FIELDS_VALIDATION`

This configuration is used in conjunction with `SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE`. If `SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE` is set to `true`, then this configuration is used to validate the unknown fields in the proto message.
Supported values are `MESSAGE`, `MESSAGE_ARRAY_FIRST_INDEX`, `MESSAGE_ARRAY_FULL`. This configuration is used to validate the unknown fields in the proto message. Check will be done recursively for the nested messages.
The choice of the value depends on the use case and trade off between performance and strong consistency.

Use cases: 
    * `MESSAGE` - Only check non repeated-message fields.
    * `MESSAGE_ARRAY_FIRST_INDEX` - Check any message type and first index of repeated message fields.
    * `MESSAGE_ARRAY_FULL` - Check any message type and all elements of repeated message field.

Scenario: 
    * schema : 
        ```
        message Test {
            string name = 1;
            int32 age = 2;
            repeated Test1 test1 = 3;
        }
        message Test1 {
            string name = 1;
            int32 age = 2;
        }
        ```
    * payload : 
        ```
        {
            "name": "test",
            "age": 10,
            "test1": [
                {
                    "name": "test1-1",
                    "age": 20,
                },
                {
                    "name": "test1-2",
                    "age": 20,
                    "unknown_field": "test"
                }
            ]
        }
        ```
    * `MESSAGE` - It will not validate the unknown field `unknown_field` in the repeated message field `test1`.
    * `MESSAGE_ARRAY_FIRST_INDEX` - Message will be validated for the unknown field `unknown_field` in the first element of repeated message field `test1`. Validation returns true since the unknown field is not present in the first element of repeated message field `test1`.
    * `MESSAGE_ARRAY_FULL` - Message will be validated for the unknown field `unknown_field` in all elements of repeated message field `test1`. Validation returns false since the unknown field is present in the second element of repeated message field `test1`.

* Example value: `MESSAGE`
* Type: `required`
* Default: `MESSAGE`


## `SINK_ADD_METADATA_ENABLED`

Defines whether to add Kafka metadata fields like topic, partition, offset, timestamp in the input proto messge.

* Example value: `true`
* Type: `optional`
* Default: `false`

## `SINK_METADATA_COLUMNS_TYPES`

Defines which Kafka metadata fields to add in the Http request payload body, along with their data types.
* Example value: `message_offset=integer,message_topic=string`
* Type: `optional`

## `SINK_DEFAULT_FIELD_VALUE_ENABLE`

Defines whether to send the default values in the request body for fields which are not present or null in the input Proto message

* Example value: `false`
* Type: `optional`
* Default value: `false`

## `METRIC_STATSD_HOST`

URL of the StatsD host

* Example value: `localhost`
* Type: `optional`
* Default value`: localhost`

## `METRIC_STATSD_PORT`

Port of the StatsD host

* Example value: `8125`
* Type: `optional`
* Default value`: 8125`

## `METRIC_STATSD_TAGS`

Global tags for StatsD metrics. Tags must be comma-separated.

* Example value: `team=engineering,app=myapp`
* Type: `optional`