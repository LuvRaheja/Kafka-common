package com.home.study.kafka.common.data;

public interface KafkaPublisherContext {
    public KafkaPayload getPayload();

    public String getTopic();
}
