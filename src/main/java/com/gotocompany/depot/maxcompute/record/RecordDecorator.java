package com.gotocompany.depot.maxcompute.record;


import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.message.Message;

import java.io.IOException;

/**
 * Decorator to process the record and message
 */
public abstract class RecordDecorator {

    private final RecordDecorator decorator;

    public RecordDecorator(RecordDecorator decorator) {
        this.decorator = decorator;
    }

    /**
     * Decorate the record with the message
     * If a nested decorator is present, it will be called to decorate the record
     *
     * @param recordWrapper record to be decorated
     * @param message depot message to be used for decoration
     * @return decorated record
     * @throws IOException if an error occurs while processing the record
     */
    public RecordWrapper decorate(RecordWrapper recordWrapper, Message message) throws IOException {
        if (decorator != null) {
            return decorator.decorate(process(recordWrapper, message), message);
        }
        return process(recordWrapper, message);
    }

    public abstract RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException;

}
