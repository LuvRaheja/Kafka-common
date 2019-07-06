package com.home.study.kafka.application;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SampleKafkaConsumer {

    @KafkaListener(id ="${kafka.topic.name:topicName}.container", topics = "testing", groupId = "testing", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String message) {
        System.out.println("Received Messasge in group foo: " + message);
    }
}
