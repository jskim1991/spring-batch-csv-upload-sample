package io.jay.batchprocessing.partitioning.config;

import io.jay.batchprocessing.common.entity.Customer;
import io.jay.batchprocessing.partitioning.partition.ColumnRangePartitioner;
import io.jay.batchprocessing.common.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final int GRID_SIZE = 5;
    private final int TOTAL_RECORD_COUNT = 1000;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> reader(@Value("#{stepExecutionContext['minValue']}") int startLine, @Value("#{stepExecutionContext['maxValue']}") int lastLine) {
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

        itemReader.setCurrentItemCount(startLine - 1);
        itemReader.setMaxItemCount(lastLine);

        return itemReader;
    }

    @Bean
    public PartitionHandler partitionHandler(@Qualifier("csvToDatabaseStep") Step step) {
        var taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(GRID_SIZE);
        taskExecutorPartitionHandler.setTaskExecutor(parititonTaskExecutor());
        taskExecutorPartitionHandler.setStep(step);
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step entryStep(PartitionHandler partitionHandler, JobRepository jobRepository) {
        return new StepBuilder("entryStep", jobRepository)
                .partitioner("csvToDatabaseStep", new ColumnRangePartitioner())
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public Step csvToDatabaseStep(FlatFileItemReader<Customer> reader, JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager, CustomerRepository customerRepository) {
        return new StepBuilder("csvToDatabaseStep", jobRepository)
                .<Customer, Customer>chunk(TOTAL_RECORD_COUNT / GRID_SIZE, transactionManager)
                .reader(reader)
                .processor(new CustomerProcessor())
                .writer(chunk -> {
                    System.out.println("Thread name: " + Thread.currentThread().getName());
                    customerRepository.saveAll(chunk.getItems());
                })
                .build();
    }

    @Bean("partitioningJob")
    public Job job(@Qualifier("entryStep") Step entryStep, JobRepository jobRepository) {
        return new JobBuilder("partitioningJob", jobRepository)
                .flow(entryStep)
                .end()
                .build();
    }


    @Bean
    public TaskExecutor parititonTaskExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(10);
        executor.setCorePoolSize(10);
        executor.setQueueCapacity(10);
        return executor;
    }
}
