package com.arnoldgalovics.blog.springbatchremotepartitioningsqs.worker;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.arnoldgalovics.blog.springbatchremotepartitioningsqs.Constants;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.integration.aws.inbound.SqsMessageDrivenChannelAdapter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.transformer.Transformer;

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
    public IntegrationFlow inboundFlow(AmazonSQSAsync sqsAsync) {
        SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(sqsAsync, Constants.QUEUE_NAME);
        return IntegrationFlows
                .from(adapter)
                .transform(jsonToObjectTransformer())
                .channel(inboundRequests())
                .get();
    }

    @Bean
    public Transformer jsonToObjectTransformer() {
        return new JsonToStepExecutionRequestTransformer();
    }

    @Bean
    public QueueChannel inboundRequests() {
        return new QueueChannel();
    }

    @Bean(name = "simpleStep")
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
