package io.jay.batchprocessing.common.controller;

import io.jay.batchprocessing.common.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final CustomerRepository customerRepository;

    @GetMapping("/count")
    public long count() {
        return customerRepository.findAll().stream().count();
    }
}
