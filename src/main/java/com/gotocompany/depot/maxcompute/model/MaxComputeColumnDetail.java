package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.type.TypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang.StringUtils;

@Data
@AllArgsConstructor
@EqualsAndHashCode
public class MaxComputeColumnDetail {
    private String prefix;
    private String name;
    private TypeInfo typeInfo;
    private boolean isArrayElement;

    public String getDdlDeclaration() {
        return String.format("%s %s", getFullName(), typeInfo.toString());
    }

    public String getFullName() {
        return StringUtils.isBlank(prefix) ? name : String.format("%s%s.%s", prefix, isArrayElement ? ".element" : "", name);
    }
}
