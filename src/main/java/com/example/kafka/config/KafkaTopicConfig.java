package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderTrackingTopic() {
        return TopicBuilder.name("order-tracking")
                .partitions(5)
                .replicas(1)
                .build();
    }
}
