package com.gotocompany.depot.config;

import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.converter.ConfToListConverter;
import com.gotocompany.depot.config.converter.MaxComputeCompressionAlgorithmConverter;
import org.aeonbits.owner.Config;

import java.util.List;

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

    @DefaultValue("true")
    @Key("SINK_MAXCOMPUTE_ADD_METADATA_ENABLED")
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

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_KEY_FORMAT")
    @DefaultValue("yyyyMMdd")
    String getTablePartitionByTimestampKeyFormat();

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
    @ConverterClass(MaxComputeCompressionAlgorithmConverter.class)
    @DefaultValue("ODPS_LZ4_FRAME")
    CompressOption.CompressAlgorithm getMaxComputeCompressionAlgorithm();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL")
    @DefaultValue("1")
    int getMaxComputeCompressionLevel();

    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY")
    @DefaultValue("0")
    int getMaxComputeCompressionStrategy();

    @Key("SINK_MAXCOMPUTE_ZONE_OFFSET")
    @DefaultValue("+07:00")
    String getZoneOffset();

}
