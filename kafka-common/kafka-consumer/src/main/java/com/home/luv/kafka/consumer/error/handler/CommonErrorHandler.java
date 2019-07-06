package com.home.luv.kafka.consumer.error.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Objects;

public class CommonErrorHandler implements BatchErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonErrorHandler.class);
    private final String containerId;
    private final KafkaListenerEndpointRegistry registry;
    private final com.home.luv.kafka.consumer.error.handler.KafkaContainerAutoRestartHandler autoRestartHandler;

    public CommonErrorHandler(String topic, KafkaListenerEndpointRegistry registry, com.home.luv.kafka.consumer.error.handler.KafkaContainerAutoRestartHandler autoRestartHandler) {
        this.containerId = String.format("%s.container", topic);
        this.registry = registry;
        this.autoRestartHandler = autoRestartHandler;
    }

    @Override
    public void handle(Exception exception, ConsumerRecords<?, ?> data) {
        LOGGER.error("Exception in message consumption {}",data, exception);
        MessageListenerContainer container = registry.getListenerContainer(containerId);
        if(Objects.nonNull(container) && container.isRunning()) {
            LOGGER.warn("Stopping {} container to avoid losing any messages", containerId);
            container.stop();
            autoRestartHandler.addStoppedContainer(containerId);
        } else {
            LOGGER.warn("Couldn't find the container with name {} or it is already stopped {} ", containerId, container == null ? false : container.isRunning());
        }


    }
}
