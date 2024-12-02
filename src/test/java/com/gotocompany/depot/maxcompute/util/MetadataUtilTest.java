package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MetadataUtilTest {

    @Test
    public void shouldReturnAppropriateStructTypeInfoForNamespacedMetadata() {
        List<TupleString> metadataColumnTypes = Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        );

        StructTypeInfo structTypeInfo = MetadataUtil.getMetadataTypeInfo(metadataColumnTypes);

        assertThat(structTypeInfo.getFieldNames()).containsExactlyInAnyOrder("__message_timestamp", "__kafka_topic", "__kafka_offset");
        assertThat(structTypeInfo.getFieldTypeInfos()).containsExactlyInAnyOrder(
                TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT
        );
    }

    @Test
    public void shouldReturnAppropriateTypeInfoForMetadataType() {
        assertThat(MetadataUtil.getMetadataTypeInfo("integer")).isEqualTo(TypeInfoFactory.INT);
        assertThat(MetadataUtil.getMetadataTypeInfo("long")).isEqualTo(TypeInfoFactory.BIGINT);
        assertThat(MetadataUtil.getMetadataTypeInfo("float")).isEqualTo(TypeInfoFactory.FLOAT);
        assertThat(MetadataUtil.getMetadataTypeInfo("double")).isEqualTo(TypeInfoFactory.DOUBLE);
        assertThat(MetadataUtil.getMetadataTypeInfo("string")).isEqualTo(TypeInfoFactory.STRING);
        assertThat(MetadataUtil.getMetadataTypeInfo("boolean")).isEqualTo(TypeInfoFactory.BOOLEAN);
        assertThat(MetadataUtil.getMetadataTypeInfo("timestamp")).isEqualTo(TypeInfoFactory.TIMESTAMP_NTZ);
    }

}
