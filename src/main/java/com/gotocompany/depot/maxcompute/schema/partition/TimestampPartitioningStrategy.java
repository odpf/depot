package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@RequiredArgsConstructor
public class TimestampPartitioningStrategy implements PartitioningStrategy {

    private static final String SECONDS_FIELD = "seconds";
    private static final String NANOS_FIELD = "nanos";
    private static final String PARTITION_SPEC_FORMAT = "%s=%s";

    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public String getOriginalPartitionColumnName() {
        return maxComputeSinkConfig.getTablePartitionKey();
    }

    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    @Override
    public Column getPartitionColumn() {
        return Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        Message message = (Message) object;
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS_FIELD));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS_FIELD));
        return new PartitionSpec(String.format(PARTITION_SPEC_FORMAT,
                maxComputeSinkConfig.getTablePartitionColumnName(), getStartOfDayEpoch(seconds, nanos)));
    }

    private String getStartOfDayEpoch(long seconds, int nanos) {
        return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC)
                .toLocalDate()
                .atStartOfDay()
                .toString();
    }

}
