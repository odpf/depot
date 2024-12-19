package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MaxComputeSink implements Sink {

    private final InsertManager insertManager;
    private final MessageRecordConverter messageRecordConverter;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    public MaxComputeSink(InsertManager insertManager,
                          MessageRecordConverter messageRecordConverter,
                          StatsDReporter statsDReporter,
                          MaxComputeMetrics maxComputeMetrics) {
        this.insertManager = insertManager;
        this.messageRecordConverter = messageRecordConverter;
        this.maxComputeMetrics = maxComputeMetrics;
        this.instrumentation = new Instrumentation(statsDReporter, this.getClass());
    }


    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        Instant conversionStartTime = Instant.now();
        RecordWrappers recordWrappers = messageRecordConverter.convert(messages);
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeConversionLatencyMetric(), conversionStartTime);
        recordWrappers.getInvalidRecords()
                .forEach(invalidRecord -> sinkResponse.getErrors().put(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        try {
            insertManager.insert(recordWrappers.getValidRecords());
        } catch (IOException | TunnelException e) {
            log.error("Error while inserting records to MaxCompute: ", e);
            sinkResponse.addErrors(recordWrappers.getValidRecords().stream().map(RecordWrapper::getIndex).collect(Collectors.toList()), new ErrorInfo(e, ErrorType.SINK_RETRYABLE_ERROR));
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, e.getClass().getSimpleName()));
        } catch (Exception e) {
            log.error("Error while inserting records to MaxCompute: ", e);
            sinkResponse.addErrors(recordWrappers.getValidRecords().stream().map(RecordWrapper::getIndex).collect(Collectors.toList()), new ErrorInfo(e, ErrorType.DEFAULT_ERROR));
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, e.getClass().getSimpleName()));
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {
    }

}
