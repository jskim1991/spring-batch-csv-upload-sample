package io.jay.batchprocessing.fileuploader.config;

import io.jay.batchprocessing.common.entity.Customer;
import io.jay.batchprocessing.common.repository.CustomerRepository;
import io.jay.batchprocessing.faulttolerant.config.FaultTolerantCustomerProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;

@Configuration
@RequiredArgsConstructor
public class UploaderBatchConfig {

    private final PlatformTransactionManager transactionManager;
    private final JobRepository jobRepository;
    private final CustomerRepository customerRepository;


    @Bean("dynamicFileReader")
    @StepScope
    public FlatFileItemReader<Customer> reader(@Value("#{jobParameters[fullPathFileName]}") String filePath) {
        var lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob", "age");

        var fieldSetMapper = new BeanWrapperFieldSetMapper<Customer>();
        fieldSetMapper.setTargetType(Customer.class);

        var lineMapper = new DefaultLineMapper<Customer>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        var itemReader = new FlatFileItemReader<Customer>();
        itemReader.setResource(new FileSystemResource(new File(filePath)));
        itemReader.setName("dynamicFileReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper);

        return itemReader;
    }

    @Bean
    public Step fileUploaderStep(@Qualifier("dynamicFileReader") FlatFileItemReader<Customer> reader) {
        return new StepBuilder("fileUploaderStep", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader)
                .processor(new FaultTolerantCustomerProcessor())
                .writer(chunk -> {
                    customerRepository.saveAll(chunk.getItems());
                })
                .build();
    }

    @Bean("fileUploaderJob")
    public Job job(@Qualifier("fileUploaderStep") Step fileUploaderStep, JobRepository jobRepository) {
        return new JobBuilder("fileUploaderJob", jobRepository)
                .flow(fileUploaderStep)
                .end()
                .build();
    }
}
