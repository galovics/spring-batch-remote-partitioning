package com.arnoldgalovics.blog.springbatchremotepartitioningsqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqsConfiguration {
    @Bean
    public AmazonSQSAsync amazonSQSAsync() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-east-1");

        AmazonSQSAsync sqsAsync = AmazonSQSAsyncClientBuilder.standard().withCredentials(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
        ).withEndpointConfiguration(endpointConfiguration).build();
        ListQueuesResult listQueuesResult = sqsAsync.listQueues(Constants.QUEUE_NAME);
        if (listQueuesResult.getQueueUrls().isEmpty()) {
            sqsAsync.createQueueAsync(Constants.QUEUE_NAME);
        }
        return sqsAsync;
    }
}
