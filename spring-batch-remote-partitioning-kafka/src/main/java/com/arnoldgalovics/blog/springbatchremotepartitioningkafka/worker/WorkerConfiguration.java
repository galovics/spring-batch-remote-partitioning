package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.worker;

import com.arnoldgalovics.blog.springbatchremotepartitioningkafka.Constants;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
@Profile("worker")
public class WorkerConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private RemotePartitioningWorkerStepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public IntegrationFlow inboundFlow(ConsumerFactory<String, String> cf) {
        return IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(cf, Constants.TOPIC_NAME))
                .channel(inboundRequests())
                .get();
    }

    @Bean
    public QueueChannel inboundRequests() {
        return new QueueChannel();
    }

    @Bean
    public Step simpleStep() {
        return stepBuilderFactory.get(Constants.WORKER_STEP_NAME)
                .inputChannel(inboundRequests())
                .<Integer, Customer>chunk(100)
                .reader(itemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public ItemWriter<Customer> itemWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .beanMapped()
                .dataSource(dataSource)
                .sql("INSERT INTO customers (id) VALUES (:id)")
                .build();
    }

    @Bean
    public ItemProcessor<Integer, Customer> itemProcessor() {
        return new ItemProcessor<>() {
            @Override
            public Customer process(Integer item) {
                return new Customer(item);
            }
        };
    }

    @Bean
    @StepScope
    public ItemReader<Integer> itemReader(@Value("#{stepExecutionContext['data']}") List<Integer> data) {
        List<Integer> remainingData = new ArrayList<>(data);
        return new ItemReader<>() {
            @Override
            public Integer read() {
                if (remainingData.size() > 0) {
                    return remainingData.remove(0);
                }

                return null;
            }
        };
    }
}
