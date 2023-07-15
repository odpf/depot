package org.raystack.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.StreamWriter;
import org.raystack.depot.bigquery.storage.BigQueryStream;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class BigQueryProtoStream implements BigQueryStream {
    @Getter
    private final StreamWriter streamWriter;
}
