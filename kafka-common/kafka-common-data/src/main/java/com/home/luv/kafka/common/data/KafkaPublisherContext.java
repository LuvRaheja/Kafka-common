package com.home.luv.kafka.common.data;

public interface KafkaPublisherContext {
    public KafkaPayload getPayload();

    public String getTopic();
}
