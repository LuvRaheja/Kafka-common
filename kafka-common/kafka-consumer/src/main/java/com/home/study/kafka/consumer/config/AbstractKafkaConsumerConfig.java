package com.home.study.kafka.consumer.config;

import com.home.study.kafka.consumer.error.handler.CommonErrorHandler;
import com.home.study.kafka.consumer.error.handler.KafkaContainerAutoRestartHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractKafkaConsumerConfig<T> {

    protected ConcurrentKafkaListenerContainerFactory<String, T> getConcurrentKafkaListenerContainerFactory(
            String bootstrapServer, String groupId, int maxPollConfig, int maxPollIntervalInMillis,
            Integer concurrency, BatchErrorHandler errorHandler, boolean isAutoStartup) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(concurrency);
        factory.setBatchErrorHandler(errorHandler);
        factory.setBatchListener(true);
        factory.setConsumerFactory(consumerFactory(bootstrapServer, groupId, maxPollConfig));
        factory.setAutoStartup(isAutoStartup);
        factory.getContainerProperties().setPollTimeout(maxPollIntervalInMillis);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.getContainerProperties().setAckOnError(false);
        return factory;
    }

    private ConsumerFactory<String,T> consumerFactory(String bootstrapServer, String groupId, int maxPollConfig) {

        return new DefaultKafkaConsumerFactory<String, T>(consumerConfig(bootstrapServer, groupId, maxPollConfig, stringKeyDeserializer(), this.payloadJsonDeserializer()));
    }

    private Map<String, Object> consumerConfig(String bootstrapServer, String groupId, int maxPollConfig, Deserializer<String> stringKeyDeserializer, Deserializer<T> payloadJsonDeserializer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollConfig);

        return props;
    }

    public abstract Deserializer<T> payloadJsonDeserializer();

    private Deserializer<String> stringKeyDeserializer() {
        return new StringDeserializer();
    }

    protected BatchErrorHandler getErrorHandler(KafkaListenerEndpointRegistry registry, String topic, KafkaContainerAutoRestartHandler autoRestartHandler) {
        return new CommonErrorHandler(topic, registry, autoRestartHandler);
    }
}
