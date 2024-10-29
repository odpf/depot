package com.gotocompany.depot.maxcompute.record;


import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.message.Message;

import java.io.IOException;

public abstract class RecordDecorator {
    private final RecordDecorator decorator;

    public RecordDecorator(RecordDecorator decorator) {
        this.decorator = decorator;
    }

    public void decorate(RecordWrapper recordWrapper, Message message) throws IOException {
        append(recordWrapper, message);
        if (decorator != null) {
            decorator.decorate(recordWrapper, message);
        }
    }

    public abstract void append(RecordWrapper recordWrapper, Message message) throws IOException;
}
