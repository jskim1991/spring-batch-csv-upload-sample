package io.jay.batchprocessing.faulttolerant.config;

import io.jay.batchprocessing.common.entity.Customer;
import io.jay.batchprocessing.faulttolerant.exception.InvalidCustomerDataException;
import org.springframework.batch.item.ItemProcessor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class FaultTolerantCustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer item) {

        validateNumber(item.getAge());

        item.setLastUpdated(Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC)));
        return item;
    }

    private void validateNumber(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException ex) {
            throw new InvalidCustomerDataException(ex.getMessage());
        }
    }
}
