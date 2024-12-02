package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.InvalidMessageException;
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
public class ProtoMessageRecordConverter implements MessageRecordConverter {

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
                        recordWrappers.addValidRecord(recordDecorator.decorate(recordWrapper, messages.get(index)));
                    } catch (IOException e) {
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR))
                        );
                    } catch (UnknownFieldsException e) {
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR))
                        );
                    } catch (InvalidMessageException e) {
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR))
                        );
                    }
                });
        return recordWrappers;
    }

    private RecordWrapper toErrorRecordWrapper(RecordWrapper recordWrapper, ErrorInfo e) {
        return new RecordWrapper(null, recordWrapper.getIndex(), e, recordWrapper.getPartitionSpec());
    }
}
