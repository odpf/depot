package io.odpf.depot.config;

public interface BigTableSinkConfig extends OdpfSinkConfig {
    @Key("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    @Key("SINK_BIGTABLE_INSTANCE_ID")
    String getInstanceId();

    @Key("SINK_BIGTABLE_TABLE_ID")
    String getTableId();

    @Key("SINK_BIGTABLE_CREDENTIAL_PATH")
    String getCredentialPath();

    @Key("SINK_BIGTABLE_ROW_KEY_TEMPLATE")
    String getRowKeyTemplate();
}
