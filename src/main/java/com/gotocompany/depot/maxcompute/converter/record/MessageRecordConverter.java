package com.gotocompany.depot.maxcompute.converter.record;

import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;

import java.util.List;

/**
 * Converts a list of messages to RecordWrappers.
 * RecordWrappers encapsulates valid and invalid records.
 * Record is the object used by MaxCompute to represent a row in a table.
 */
public interface MessageRecordConverter {
    RecordWrappers convert(List<Message> messages);
}
