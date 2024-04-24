package com.gotocompany.depot.http.response;

import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

public class HttpSinkResponse {
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    private boolean isFail;
    private int responseCode;
    private String responseBody;
    private boolean shouldLogResponse;

    public HttpSinkResponse(HttpResponse response, Instrumentation instrumentation) throws IOException {
        setIsFail(response);
        setResponseCode(response);
        setShouldLogResponse(instrumentation);
        setResponseBody(response);
    }

    private void setIsFail(HttpResponse response) {
        isFail = true;
        if (hasStatusLine(response)) {
            isFail = !Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches();
        }
    }

    private void setResponseCode(HttpResponse response) {
        if (hasStatusLine(response)) {
            responseCode = response.getStatusLine().getStatusCode();
        } else {
            responseCode = -1;
        }
    }

    private static boolean hasStatusLine(HttpResponse response) {
        return response != null && response.getStatusLine() != null;
    }

    private void setResponseBody(HttpResponse response) throws IOException {
        if (!hasResponse(response)) {
            return;
        }
        if (shouldLogResponse || isFail) {
            responseBody = EntityUtils.toString(response.getEntity());
        } else {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    public String getResponseBody() {
        return responseBody;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public boolean isFail() {
        return isFail;
    }

    public boolean shouldLogResponse() {
        return shouldLogResponse;
    }

    public void setShouldLogResponse(Instrumentation instrumentation) {
        shouldLogResponse = instrumentation.isDebugEnabled();
    }

    private boolean hasResponse(HttpResponse response) {
        return Objects.nonNull(response) && Objects.nonNull(response.getEntity());
    }
}
