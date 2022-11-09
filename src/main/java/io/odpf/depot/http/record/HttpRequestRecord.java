package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.http.response.HttpSinkResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

@AllArgsConstructor
@Getter
public class HttpRequestRecord {

    private final Long index;
    private final ErrorInfo errorInfo;
    private final boolean valid;
    private HttpEntityEnclosingRequestBase httpRequest;

    public HttpSinkResponse send(HttpClient httpClient) throws IOException {
        HttpResponse response = httpClient.execute(this.httpRequest);
        return new HttpSinkResponse(response);
    }

    public String getRequestBody() {
        try {
            return EntityUtils.toString(httpRequest.getEntity());
        } catch (Exception e) {
            return "null";
        }
    }
}
