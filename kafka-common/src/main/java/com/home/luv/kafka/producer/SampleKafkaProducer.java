package com.home.luv.kafka.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class SampleKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final String topic;

    SampleKafkaProducer(KafkaTemplate<String, String> kafkaTemplate, @Value("${kafka.topic:testing}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @RequestMapping("/sendMessage/{message}")
    public String sendMessage (@PathVariable("message") String message) {
        kafkaTemplate.send(topic, UUID.randomUUID().toString(), message);
        return "messageSent";
    }

    @RequestMapping("/")
    public String sendHi () {
        kafkaTemplate.send(topic, "Hi");
        return "messageSent";
    }

}
