package com.gotocompany.depot.http.response;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponseParser {
    private static final int MIN_BAD_REQUEST_CODE = 400;
    private static final int MAX_BAD_REQUEST_CODE = 499;
    private static final int MIN_SERVER_ERROR_CODE = 500;
    private static final int MAX_SERVER_ERROR_CODE = 599;

    public static Map<Long, ErrorInfo> getErrorsFromResponse(
            List<HttpRequestRecord> records,
            List<HttpSinkResponse> responses,
            Map<Integer, Boolean> retryStatusCodeRanges,
            Map<Integer, Boolean> requestLogStatusCodeRanges,
            Instrumentation instrumentation) throws IOException {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int i = 0; i < responses.size(); i++) {
            HttpRequestRecord record = records.get(i);
            HttpSinkResponse response = responses.get(i);
            int responseCode = response.getResponseCode();
            instrumentation.logInfo("Response Status: {}", responseCode);
            if (shouldLogRequest(responseCode, requestLogStatusCodeRanges)) {
                instrumentation.logInfo(record.getRequestString());
            }
            if (response.shouldLogResponse()) {
                instrumentation.logDebug(response.getResponseBody());
            }
            if (response.isFail()) {
                errors.putAll(getErrors(record, responseCode, retryStatusCodeRanges));
                instrumentation.logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", responseCode, response.getResponseBody());
            }
        }
        return errors;
    }

    private static Map<Long, ErrorInfo> getErrors(HttpRequestRecord record, int responseCode, Map<Integer, Boolean> retryStatusCodeRanges) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (long messageIndex : record) {
            if (retryStatusCodeRanges.containsKey(responseCode)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (isResponseCodeInRange(responseCode, MIN_BAD_REQUEST_CODE, MAX_BAD_REQUEST_CODE)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_4XX_ERROR));
            } else if (isResponseCodeInRange(responseCode, MIN_SERVER_ERROR_CODE, MAX_SERVER_ERROR_CODE)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_5XX_ERROR));
            } else {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_UNKNOWN_ERROR));
            }
        }
        return errors;
    }

    private static boolean isResponseCodeInRange(int responseCode, int minRange, int maxRange) {
        return responseCode >= minRange && responseCode <= maxRange;
    }

    private static boolean shouldLogRequest(int responseCode, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        return responseCode == -1 || requestLogStatusCodeRanges.containsKey(responseCode);
    }
}
