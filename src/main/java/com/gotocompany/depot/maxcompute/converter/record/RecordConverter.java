package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

@RequiredArgsConstructor
public class RecordConverter implements MessageRecordConverter {

    private final RecordDecorator recordDecorator;
    private final MaxComputeSchemaCache maxComputeSchemaCache;

    @Override
    public RecordWrappers convert(List<Message> messages) {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        RecordWrappers recordWrappers = new RecordWrappers();
        IntStream.range(0, messages.size())
                .forEach(index -> {
                    Record record = new ArrayRecord(maxComputeSchema.getColumns());
                    RecordWrapper recordWrapper = new RecordWrapper(record, index, null, null);
                    try {
                        recordDecorator.decorate(recordWrapper, messages.get(index));
                        recordWrappers.addValidRecord(recordWrapper);
                    } catch (IOException e) {
                        handleException(recordWrapper, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR), recordWrappers);
                    } catch (UnknownFieldsException e) {
                        handleException(recordWrapper, new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR), recordWrappers);
                    }
                });
        return recordWrappers;
    }

    private void handleException(RecordWrapper recordWrapper, ErrorInfo e, RecordWrappers recordWrappers) {
        recordWrapper.setRecord(null);
        recordWrapper.setErrorInfo(e);
        recordWrappers.addInvalidRecord(recordWrapper);
    }
}
