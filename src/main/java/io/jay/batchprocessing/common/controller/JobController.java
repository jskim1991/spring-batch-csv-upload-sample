package io.jay.batchprocessing.common.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class JobController {

    private final JobLauncher jobLauncher;

    @Qualifier("partitioningJob")
    private final Job partitioningJob;

    @Qualifier("faultTolerantJob")
    private final Job faultTolerantJob;

    @GetMapping("/v1/run")
    public void startV1() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            jobLauncher.run(partitioningJob, jobParameters);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @GetMapping("/v2/run")
    public void startV2() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            jobLauncher.run(faultTolerantJob, jobParameters);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
}
