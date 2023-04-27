package io.jay.batchprocessing.common.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@RestController
@RequiredArgsConstructor
public class JobController {

    private final JobLauncher jobLauncher;

    @Qualifier("partitioningJob")
    private final Job partitioningJob;

    @Qualifier("faultTolerantJob")
    private final Job faultTolerantJob;

    @Qualifier("fileUploaderJob")
    private final Job fileUploaderJob;

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

    @GetMapping("/v3/run")
    public void startV3(@RequestParam("file") MultipartFile multipartFile) throws IOException {
        File tempFile = new File("/tmp/" + multipartFile.getOriginalFilename());
        multipartFile.transferTo(tempFile);

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .addString("fullPathFileName", tempFile.getAbsolutePath())
                .toJobParameters();

        try {
            JobExecution jobExecution = jobLauncher.run(fileUploaderJob, jobParameters);
            if (ExitStatus.FAILED.equals(jobExecution.getExitStatus())) {
                throw new RuntimeException("Job failed");
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }
}
