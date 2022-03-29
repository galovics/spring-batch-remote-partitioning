package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import com.arnoldgalovics.blog.springbatchremotepartitioningkafka.Constants;
import java.util.function.Function;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;

@Configuration
@Profile("manager")
public class ManagerConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private RemotePartitioningManagerStepBuilderFactory stepBuilderFactory;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Bean
    public DirectChannel outboundRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow() {
        KafkaProducerMessageHandler messageHandler = new KafkaProducerMessageHandler(kafkaTemplate);
        messageHandler.setTopicExpression(new LiteralExpression(Constants.TOPIC_NAME));
        Function<Message<?>, Long> partitionIdFn = (m) -> {
            StepExecutionRequest executionRequest = (StepExecutionRequest) m.getPayload();
            return executionRequest.getStepExecutionId() % Constants.TOPIC_PARTITION_COUNT;
        };
        messageHandler.setPartitionIdExpression(new FunctionExpression<>(partitionIdFn));
        return IntegrationFlows
                .from(outboundRequests())
                .log()
                .handle(messageHandler)
                .get();
    }

    @Bean(name = "partitionerJob")
    public Job partitionerJob() {
        return jobBuilderFactory.get("partitioningJob")
                .start(partitionerStep())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public ExamplePartitioner partitioner() {
        return new ExamplePartitioner();
    }

    @Bean
    public Step partitionerStep() {
        return stepBuilderFactory.get("partitionerStep")
                .partitioner(Constants.WORKER_STEP_NAME, partitioner())
                .outputChannel(outboundRequests())
                .build();
    }
}
