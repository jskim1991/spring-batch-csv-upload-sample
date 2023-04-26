package io.jay.batchprocessing.repository;

import io.jay.batchprocessing.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Integer> {
}
