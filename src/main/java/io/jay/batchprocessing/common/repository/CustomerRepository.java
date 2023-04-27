package io.jay.batchprocessing.common.repository;

import io.jay.batchprocessing.common.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Integer> {
}
