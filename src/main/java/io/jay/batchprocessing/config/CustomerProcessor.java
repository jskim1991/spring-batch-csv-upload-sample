package io.jay.batchprocessing.config;

import io.jay.batchprocessing.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer item) {
        /* return null if you want to filter */
        item.setLastUpdated(Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC)));
        return item;
    }
}
