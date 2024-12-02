package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TableValidatorTest {

    private TableValidator tableValidator;

    @Before
    public void init() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getTableValidatorNameRegex()).thenReturn("^[a-zA-Z_][a-zA-Z0-9_]{0,29}$");
        Mockito.when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable()).thenReturn(1200);
        Mockito.when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable()).thenReturn(6);
        tableValidator = new TableValidator(maxComputeSinkConfig);
    }

    @Test
    public void shouldValidateValidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("ValidTableName", null, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateInvalidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("1InvalidTableName", 30L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateNegativeLifecycleDays() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("ValidTableName", -1L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxColumns() {
        TableSchema tableSchema = new TableSchema();
        for (int i = 0; i < 1201; i++) {
            tableSchema.addColumn(new Column("column" + i, TypeInfoFactory.STRING));
        }
        tableValidator.validate("ValidTableName", 30L, tableSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxPartitionKeys() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        for (int i = 0; i < 7; i++) {
            tableSchema.addPartitionColumn(new Column("partition" + i, TypeInfoFactory.STRING));
        }
        tableValidator.validate("ValidTableName", 30L, tableSchema);
    }

}
