package io.odpf.depot.bigquery.exception;

public class BQDatasetLocationChangedException extends RuntimeException {
    public BQDatasetLocationChangedException(String message) {
        super(message);
    }
}

