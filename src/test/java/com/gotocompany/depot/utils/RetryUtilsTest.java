package com.gotocompany.depot.utils;

import com.gotocompany.depot.exception.NonRetryableException;
import org.junit.Test;
import org.mockito.Mockito;

public class RetryUtilsTest {

    @Test
    public void shouldRetryUntilSuccess() throws Exception {
        int repeatCountBeforeSuccess = 3;
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(repeatCountBeforeSuccess));

        RetryUtils.executeWithRetry(runnableMock::execute, 5, 0, e -> e instanceof Exception);

        Mockito.verify(runnableMock, Mockito.times(repeatCountBeforeSuccess)).execute();
    }

    @Test(expected = NonRetryableException.class)
    public void shouldThrowNonRetryableExceptionAfterRetryIsExhausted() throws Exception {
        int repeatCountBeforeSuccess = 5;
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(repeatCountBeforeSuccess));

        RetryUtils.executeWithRetry(runnableMock::execute, 3, 0, e -> e instanceof RuntimeException);

        Mockito.verify(runnableMock, Mockito.times(3)).execute();
    }

    @Test(expected = NonRetryableException.class)
    public void shouldThrowNonRetryableExceptionWhenNonMatchingExceptionIsThrown() throws Exception {
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(3));

        RetryUtils.executeWithRetry(runnableMock::execute, 3, 0, e -> e instanceof IllegalArgumentException);

        Mockito.verify(runnableMock, Mockito.times(1)).execute();
    }

    private static class RunnableMock {
        private final int repeatCountBeforeSuccess;
        private int repeatCount;

        RunnableMock(int repeatCountBeforeSuccess) {
            this.repeatCountBeforeSuccess = repeatCountBeforeSuccess;
            this.repeatCount = 0;
        }

        void execute() throws Exception {
            repeatCount++;
            if (repeatCount < repeatCountBeforeSuccess) {
                throw new Exception("Mock exception");
            }
        }
    }

}
