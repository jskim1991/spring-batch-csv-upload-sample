package io.jay.batchprocessing.faulttolerant.config;

import io.jay.batchprocessing.faulttolerant.exception.InvalidCustomerDataException;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

public class CustomExceptionSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        if (t instanceof InvalidCustomerDataException) {
            return true;
        }
        return false;
    }
}
