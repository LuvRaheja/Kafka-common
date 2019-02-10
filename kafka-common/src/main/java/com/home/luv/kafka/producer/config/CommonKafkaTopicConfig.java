package com.home.luv.kafka.producer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CommonKafkaTopicConfig {


    private final String bootstrapServer;

    CommonKafkaTopicConfig(@Value("${kafka.bootstrap.server}")String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;

    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return new KafkaAdmin(configs);
    }

    /*@Bean
    public NewTopic testKafkaTopic() {
        return new NewTopic("testing", 1, (short) 1);
    }*/

}
