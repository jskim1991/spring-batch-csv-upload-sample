package io.jay.batchprocessing.config;

import io.jay.batchprocessing.entity.Customer;
import io.jay.batchprocessing.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository jobRepository;
    private final CustomerRepository customerRepository;

    private final PlatformTransactionManager transactionManager;

    public FlatFileItemReader<Customer> reader() {
        var lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        var fieldSetMapper = new BeanWrapperFieldSetMapper<Customer>();
        fieldSetMapper.setTargetType(Customer.class);

        var lineMapper = new DefaultLineMapper<Customer>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        var itemReader = new FlatFileItemReader<Customer>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper);

        return itemReader;
    }

    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    public RepositoryItemWriter<Customer> writer() {
        var writer = new RepositoryItemWriter<Customer>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step stepOne() {
        return new StepBuilder("csv-step", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job job() {
        return new JobBuilder("job", jobRepository)
                .flow(stepOne())
                .end()
                .build();
    }

    public TaskExecutor taskExecutor() {
        var executor = new SimpleAsyncTaskExecutor();
        executor.setConcurrencyLimit(5);
        return executor;
    }
}
