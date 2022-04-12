package com.arnoldgalovics.blog.springbatchremotepartitioningkafka;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerSeekAware;

@Configuration
public class KafkaConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(Constants.TOPIC_NAME)
                .partitions(Constants.TOPIC_PARTITION_COUNT)
                .build();
    }

    @Bean
    public ConsumerSeekAware rebalanceListener() {
        return new ConsumerSeekAware() {
            @Override
            public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
                Set<TopicPartition> partitions = assignments.keySet();
                logger.info("Setting offset to beginning for partitions {}", partitions);
                callback.seekToBeginning(partitions);
            }
        };
    }
}
