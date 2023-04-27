package io.jay.batchprocessing.faulttolerant.config;

import io.jay.batchprocessing.common.entity.Customer;
import io.jay.batchprocessing.faulttolerant.listener.FaultTolerantSkipListener;
import io.jay.batchprocessing.common.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class FaultTolerantBatchConfig {

    private final PlatformTransactionManager transactionManager;
    private final JobRepository jobRepository;
    private final CustomerRepository customerRepository;


    public FlatFileItemReader<Customer> reader() {
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
        itemReader.setResource(new FileSystemResource("src/main/resources/customers-bad.csv"));
        itemReader.setName("badCsvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper);

        return itemReader;
    }

    @Bean
    public Step faultTolerantStep() {
        return new StepBuilder("faultTolerantStep", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader())
                .processor(new FaultTolerantCustomerProcessor())
                .writer(chunk -> {
                    customerRepository.saveAll(chunk.getItems());
                })
                .faultTolerant()
                // can also use  .skipLimit(3).skip(InvalidCustomerDataException.class)
                // can also add  .noSkip(SomeException.class)
                .listener(skipListener())
                .skipPolicy(new CustomExceptionSkipPolicy())
                .build();
    }

    @Bean("faultTolerantJob")
    public Job job(@Qualifier("faultTolerantStep") Step faultTolerantStep, JobRepository jobRepository) {
        return new JobBuilder("faultTolerantJob", jobRepository)
                .flow(faultTolerantStep)
                .end()
                .build();
    }

    @Bean
    public SkipListener skipListener() {
        return new FaultTolerantSkipListener();
    }
}
