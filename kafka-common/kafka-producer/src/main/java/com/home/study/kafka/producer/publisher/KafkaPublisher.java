package com.home.study.kafka.producer.publisher;

import com.home.study.kafka.common.data.KafkaPublisherContext;
import com.home.study.kafka.producer.callback.KafkaPublisherCallback;

public interface KafkaPublisher {

    boolean publish(KafkaPublisherContext context);

    boolean publishAsync(KafkaPublisherContext context, KafkaPublisherCallback callback);
}
