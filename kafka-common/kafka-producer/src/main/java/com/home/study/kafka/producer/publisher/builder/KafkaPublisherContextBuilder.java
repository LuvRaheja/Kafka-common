package com.home.study.kafka.producer.publisher.builder;

import com.home.study.kafka.common.data.KafkaPayload;
import com.home.study.kafka.common.data.KafkaPublisherContext;
import com.home.study.kafka.data.load.constant.PayloadType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

@Component
public class KafkaPublisherContextBuilder {

    private final Map<PayloadType, String> topicNameMapping;

    public KafkaPublisherContextBuilder(@Value("${kafka.topic.topicName:topicName}")String topicName) {
        this.topicNameMapping = new EnumMap<PayloadType, String>(PayloadType.class);
        topicNameMapping.put(PayloadType.TWITTER, topicName);
    }

    public KafkaPublisherContext buildAsyncContext(PayloadType payloadType, KafkaPayload payload) {
        String topicName = getTopic(payloadType);
        return new KafkaPublisherContext() {
            @Override
            public KafkaPayload getPayload() {
                return payload;
            }

            @Override
            public String getTopic() {
                return topicName;
            }
        };
    }

    private String getTopic(PayloadType payloadType) {
        if(!topicNameMapping.containsKey(payloadType)) {
            throw new RuntimeException("No Kafka topic mapped for Payload Type " + payloadType);
        }
        return topicNameMapping.get(payloadType);
    }
}
