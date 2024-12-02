package com.gotocompany.depot.config;

import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.converter.ConfToListConverter;
import com.gotocompany.depot.config.converter.LocalDateTimeConverter;
import com.gotocompany.depot.config.converter.MaxComputeOdpsGlobalSettingsConverter;
import com.gotocompany.depot.config.converter.ZoneIdConverter;
import org.aeonbits.owner.Config;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public interface MaxComputeSinkConfig extends Config {

    @Key("SINK_MAXCOMPUTE_ODPS_URL")
    String getMaxComputeOdpsUrl();

    @Key("SINK_MAXCOMPUTE_TUNNEL_URL")
    String getMaxComputeTunnelUrl();

    @Key("SINK_MAXCOMPUTE_ACCESS_ID")
    String getMaxComputeAccessId();

    @Key("SINK_MAXCOMPUTE_ACCESS_KEY")
    String getMaxComputeAccessKey();

    @Key("SINK_MAXCOMPUTE_PROJECT_ID")
    String getMaxComputeProjectId();

    @Key("SINK_MAXCOMPUTE_METADATA_NAMESPACE")
    @DefaultValue("")
    String getMaxcomputeMetadataNamespace();

    @Key("SINK_MAXCOMPUTE_ADD_METADATA_ENABLED")
    @DefaultValue("true")
    boolean shouldAddMetadata();

    @DefaultValue("")
    @Key("SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getMetadataColumnsTypes();

    @Key("SINK_MAXCOMPUTE_SCHEMA")
    @DefaultValue("default")
    String getMaxComputeSchema();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME")
    String getTablePartitionColumnName();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_TIME_UNIT")
    @DefaultValue("DAY")
    String getTablePartitionByTimestampTimeUnit();

    @Key("SINK_MAXCOMPUTE_TABLE_NAME")
    String getMaxComputeTableName();

    @Key("SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS")
    Long getMaxComputeTableLifecycleDays();

    @Key("SINK_MAXCOMPUTE_RECORD_PACK_FLUSH_TIMEOUT_MS")
    @DefaultValue("-1")
    Long getMaxComputeRecordPackFlushTimeoutMs();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ENABLED")
    @DefaultValue("false")
    boolean isStreamingInsertCompressEnabled();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ALGORITHM")
    @DefaultValue("ODPS_LZ4_FRAME")
    CompressOption.CompressAlgorithm getMaxComputeCompressionAlgorithm();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL")
    @DefaultValue("1")
    int getMaxComputeCompressionLevel();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY")
    @DefaultValue("0")
    int getMaxComputeCompressionStrategy();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_MAXIMUM_SESSION_COUNT")
    @DefaultValue("2")
    int getStreamingInsertMaximumSessionCount();

    @Key("SINK_MAXCOMPUTE_ZONE_ID")
    @ConverterClass(ZoneIdConverter.class)
    @DefaultValue("Asia/Bangkok")
    ZoneId getZoneId();

    @Key("SINK_MAXCOMPUTE_MAX_DDL_RETRY_COUNT")
    @DefaultValue("10")
    int getMaxDdlRetryCount();

    @Key("SINK_MAXCOMPUTE_DDL_RETRY_BACKOFF_MILLIS")
    @DefaultValue("1000")
    long getDdlRetryBackoffMillis();

    @Key("SINK_MAXCOMPUTE_ODPS_GLOBAL_SETTINGS")
    @ConverterClass(MaxComputeOdpsGlobalSettingsConverter.class)
    @DefaultValue("odps.schema.evolution.enable=true,odps.namespace.schema=true")
    Map<String, String> getOdpsGlobalSettings();

    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_NAME_REGEX")
    @DefaultValue("^[A-Za-z][A-Za-z0-9_]{0,127}$")
    String getTableValidatorNameRegex();

    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_COLUMNS_PER_TABLE")
    @DefaultValue("1200")
    int getTableValidatorMaxColumnsPerTable();

    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_PARTITION_KEYS_PER_TABLE")
    @DefaultValue("6")
    int getTableValidatorMaxPartitionKeysPerTable();

    @Key("SINK_MAXCOMPUTE_VALID_MIN_TIMESTAMP")
    @ConverterClass(LocalDateTimeConverter.class)
    @DefaultValue("1970-01-01T00:00:00")
    LocalDateTime getValidMinTimestamp();

    @Key("SINK_MAXCOMPUTE_VALID_MAX_TIMESTAMP")
    @ConverterClass(LocalDateTimeConverter.class)
    @DefaultValue("9999-12-31T23:59:59")
    LocalDateTime getValidMaxTimestamp();

}
