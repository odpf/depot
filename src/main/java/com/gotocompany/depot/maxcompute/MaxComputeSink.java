package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class MaxComputeSink implements Sink {

    private final MaxComputeClient maxComputeClient;
    private final MessageRecordConverter messageRecordConverter;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        Instant conversionStartTime = Instant.now();
        RecordWrappers recordWrappers = messageRecordConverter.convert(messages);
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeConversionLatencyMetric(), conversionStartTime);
        recordWrappers.getInvalidRecords()
                .forEach(invalidRecord -> sinkResponse.getErrors().put(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        try {
            maxComputeClient.insert(recordWrappers.getValidRecords());
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
