package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.Test;

public class TableValidatorTest {

    @Test
    public void shouldValidateValidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        TableValidator.validate("ValidTableName", null, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateInvalidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        TableValidator.validate("1InvalidTableName", 30L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateNegativeLifecycleDays() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        TableValidator.validate("ValidTableName", -1L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxColumns() {
        TableSchema tableSchema = new TableSchema();
        for (int i = 0; i < 1201; i++) {
            tableSchema.addColumn(new Column("column" + i, TypeInfoFactory.STRING));
        }
        TableValidator.validate("ValidTableName", 30L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxPartitionKeys() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        for (int i = 0; i < 7; i++) {
            tableSchema.addPartitionColumn(new Column("partition" + i, TypeInfoFactory.STRING));
        }
        TableValidator.validate("ValidTableName", 30L, tableSchema);
    }

}
