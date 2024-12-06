package com.gotocompany.depot.maxcompute.record;


import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.message.Message;

import java.io.IOException;

public abstract class RecordDecorator {

    private final RecordDecorator decorator;

    public RecordDecorator(RecordDecorator decorator) {
        this.decorator = decorator;
    }

    public RecordWrapper decorate(RecordWrapper recordWrapper, Message message) throws IOException {
        if (decorator != null) {
            return decorator.decorate(process(recordWrapper, message), message);
        }
        return process(recordWrapper, message);
    }

    public abstract RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException;

}
