package com.gotocompany.depot.utils;

import com.gotocompany.depot.exception.NonRetryableException;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Predicate;

@Slf4j
public class RetryUtils {

    public static void executeWithRetry(RunnableWithException runnableWithException,
                                        int maxRetries,
                                        long backoffMillis,
                                        Predicate<Exception> retryPredicate) {
        int retryCount = 0;
        Exception lastException = null;
        while (retryCount < maxRetries) {
            try {
                runnableWithException.run();
                break;
            } catch (Exception e) {
                if (retryPredicate.test(e)) {
                    retryCount++;
                    lastException = e;
                    log.info("Retrying operation, retry count: {}", retryCount);
                } else {
                    log.error("Non-retryable exception occurred, aborting operation", e);
                    throw new NonRetryableException(e.getMessage(), e);
                }
            }
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                log.error("Thread interrupted while sleeping", e);
            }
        }
        if (retryCount == maxRetries) {
            log.error("Max retries reached, aborting operation");
            throw new NonRetryableException("Max retries reached, aborting operation", lastException);
        }
    }

    @FunctionalInterface
    public interface RunnableWithException {
        void run() throws Exception;
    }

}
