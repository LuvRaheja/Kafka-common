package com.home.study.kafka.producer.publisher.config;

import com.home.study.kafka.common.data.KafkaPayload;
import com.home.study.kafka.producer.publisher.GenericKafkaPublisher;
import com.home.study.kafka.producer.publisher.KafkaPublisher;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CommonKafkaProducerConfig {
    @Value("${kafka.bootstrap.server}")
    private String bootstrapServer;

    @Value("${kafka.buffer.memory.size:125000000}")
    private long bufferMemorySize;

    @Value("${kafka.batch.size:100000}")
    private int batchSize;

    @Value("${kafka.max.request.size:10000000}")
    private int maxRequestSize;

    @Value("${kafka.producer.retries:3}")
    private int producerRetries;

    @Value("${kafka.producer.linger.delay.ms:500}")
    private int lingerDelayMs;

    @Value("${kafka.producer.request.timeout:30000}")
    private int requestTimeoutConfig;

    @Value("${kafka.producer.client.config:producercluster}")
    private String clientConfig;

    @Bean("asyncKafkaPublisher")
    public KafkaPublisher asyncKafkaPublisher() {
        return new GenericKafkaPublisher(kafkaTemplate());
    }


    @Bean
    public ProducerFactory<String, KafkaPayload> producerFactory() {
        return new DefaultKafkaProducerFactory<>(getProducerConfigs(), stringKeySerializer(), jsonSerializer());//jsonSerializer());
    }

    private Serializer<KafkaPayload> jsonSerializer() {
        JsonSerializer<KafkaPayload> valueSerializer = new JsonSerializer<KafkaPayload>();
        valueSerializer.setAddTypeInfo(false);
        return valueSerializer;
    }

    private Serializer<String> stringKeySerializer() {
        return new StringSerializer();
    }

    private Map<String, Object> getProducerConfigs() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientConfig);
        configProps.put(ProducerConfig.RETRIES_CONFIG, producerRetries);
                configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
                configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerDelayMs);
                configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutConfig);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemorySize);
        return configProps;
    }

    @Bean
    public KafkaTemplate<String, KafkaPayload> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
