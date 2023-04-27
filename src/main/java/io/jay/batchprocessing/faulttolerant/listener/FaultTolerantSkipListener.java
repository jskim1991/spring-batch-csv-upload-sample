package io.jay.batchprocessing.faulttolerant.listener;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jay.batchprocessing.common.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j
public class FaultTolerantSkipListener implements SkipListener<Customer, Number> {
    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Failed to read due to {}", t.getMessage());
    }

    @Override
    public void onSkipInWrite(Number item, Throwable t) {
        log.warn("Failed to write {} due to {}", item, t.getMessage());
    }

    @Override
    public void onSkipInProcess(Customer item, Throwable t) {
        log.warn("Failed to process {} due to {}", logItem(item), t.getMessage());
    }

    private String logItem(Customer item) {
        try {
            return new ObjectMapper().writeValueAsString(item);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
