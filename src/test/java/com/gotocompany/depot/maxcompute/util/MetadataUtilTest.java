package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MetadataUtilTest {

    @Test
    public void shouldReturnAppropriateStructTypeInfoForNamespacedMetadata() {
        List<TupleString> metadataColumnTypes = Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        );

        StructTypeInfo structTypeInfo = MetadataUtil.getMetadataTypeInfo(metadataColumnTypes);

        Assertions.assertThat(structTypeInfo.getFieldNames()).containsExactlyInAnyOrder("__message_timestamp", "__kafka_topic", "__kafka_offset");
        Assertions.assertThat(structTypeInfo.getFieldTypeInfos()).containsExactlyInAnyOrder(
                TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT
        );
    }

    @Test
    public void shouldReturnAppropriateTypeInfoForMetadataType() {
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("integer")).isEqualTo(TypeInfoFactory.INT);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("long")).isEqualTo(TypeInfoFactory.BIGINT);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("float")).isEqualTo(TypeInfoFactory.FLOAT);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("double")).isEqualTo(TypeInfoFactory.DOUBLE);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("string")).isEqualTo(TypeInfoFactory.STRING);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("boolean")).isEqualTo(TypeInfoFactory.BOOLEAN);
        Assertions.assertThat(MetadataUtil.getMetadataTypeInfo("timestamp")).isEqualTo(TypeInfoFactory.TIMESTAMP_NTZ);
    }

}
