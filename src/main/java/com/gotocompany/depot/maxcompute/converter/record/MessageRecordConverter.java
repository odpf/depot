package com.gotocompany.depot.maxcompute.converter.record;

import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;

import java.util.List;

public interface MessageRecordConverter {
    RecordWrappers convert(List<Message> messages);
}
