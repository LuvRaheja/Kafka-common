package com.home.luv.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SampleKafkaConsumer {

    @KafkaListener(topics = "testing", groupId = "testing")
    public void listen(String message) {
        System.out.println("Received Messasge in group foo: " + message);
    }
}
