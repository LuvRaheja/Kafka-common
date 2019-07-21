package com.home.study.kafka.common.data;

public class SimpleKafkaPayload implements KafkaPayload {
    private final String message;

    public SimpleKafkaPayload(String message) {
        this.message = message;
    }

    @Override
    public String getMessageId() {
        return message;
    }

}
