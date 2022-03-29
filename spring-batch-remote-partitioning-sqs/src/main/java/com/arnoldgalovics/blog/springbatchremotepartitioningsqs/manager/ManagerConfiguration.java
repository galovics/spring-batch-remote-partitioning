package com.arnoldgalovics.blog.springbatchremotepartitioningsqs.manager;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.arnoldgalovics.blog.springbatchremotepartitioningsqs.Constants;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.aws.outbound.SqsMessageHandler;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.json.ObjectToJsonTransformer;

@Configuration
@Profile("manager")
public class ManagerConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private RemotePartitioningManagerStepBuilderFactory stepBuilderFactory;

    @Bean
    public DirectChannel outboundRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(AmazonSQSAsync sqsAsync) {
        SqsMessageHandler sqsMessageHandler = new SqsMessageHandler(sqsAsync);
        sqsMessageHandler.setQueue(Constants.QUEUE_NAME);
        return IntegrationFlows.from(outboundRequests())
                .transform(objectToJsonTransformer())
                .log()
                .handle(sqsMessageHandler)
                .get();
    }

    @Bean
    public ObjectToJsonTransformer objectToJsonTransformer() {
        return new ObjectToJsonTransformer();
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
