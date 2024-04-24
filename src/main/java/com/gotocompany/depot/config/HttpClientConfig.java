package com.gotocompany.depot.config;


public interface HttpClientConfig extends SinkConfig {

    @Key("SINK_HTTPV2_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getHttpMaxConnections();

    @Key("SINK_HTTPV2_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getHttpRequestTimeoutMs();

    @Key("SINK_HTTPV2_OAUTH2_ENABLE")
    @DefaultValue("false")
    Boolean isHttpOAuth2Enable();

    @Key("SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL")
    String getHttpOAuth2AccessTokenUrl();

    @Key("SINK_HTTPV2_OAUTH2_CLIENT_NAME")
    String getHttpOAuth2ClientName();

    @Key("SINK_HTTPV2_OAUTH2_CLIENT_SECRET")
    String getHttpOAuth2ClientSecret();

    @Key("SINK_HTTPV2_OAUTH2_SCOPE")
    String getHttpOAuth2Scope();
}
