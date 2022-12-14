package io.odpf.depot.http.request.builder;

import io.odpf.depot.common.Template;
import io.odpf.depot.common.TemplateUtils;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpParameterSourceType;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessageParser;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryParamBuilder {

    private final Map<Template, Template> queryParamTemplates;
    private final HttpParameterSourceType queryParameterSource;
    private final String schemaProtoKeyClass;
    private final String schemaProtoMessageClass;


    public QueryParamBuilder(HttpSinkConfig config) {
        this.queryParamTemplates = config.getQueryTemplate();
        this.queryParameterSource = config.getQueryParamSourceMode();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    public Map<String, String> build() {
        return queryParamTemplates
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        templateKey -> templateKey.getKey().getTemplatePattern(),
                        templateValue -> templateValue.getValue().getTemplatePattern()
                ));
    }

    public Map<String, String> build(MessageContainer container, OdpfMessageParser odpfMessageParser) throws IOException {
        if (queryParameterSource == HttpParameterSourceType.KEY) {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    container.getParsedLogKey(odpfMessageParser, schemaProtoKeyClass),
                    odpfMessageParser.getSchema(schemaProtoKeyClass)
            );
        } else {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    container.getParsedLogMessage(odpfMessageParser, schemaProtoMessageClass),
                    odpfMessageParser.getSchema(schemaProtoMessageClass)
            );
        }
    }
}
