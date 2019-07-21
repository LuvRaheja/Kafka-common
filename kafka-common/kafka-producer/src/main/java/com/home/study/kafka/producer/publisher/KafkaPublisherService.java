package com.home.study.kafka.producer.publisher;

import com.google.common.collect.Lists;
import com.home.study.kafka.common.data.KafkaPayload;
import com.home.study.kafka.common.data.KafkaPublisherContext;
import com.home.study.kafka.data.load.constant.SourceContext;
import com.home.study.kafka.producer.callback.KafkaPublisherCallback;
import com.home.study.kafka.producer.publisher.builder.KafkaPublisherContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

@Service
public class KafkaPublisherService {

    private final KafkaPublisher publisher;
    private final KafkaPublisherContextBuilder kafkaPublisherContextBuilder;
    private final int maxRecordsInPayload;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisherService.class);
    private final KafkaPublisherCallback kafkaPublisherCallback;

    public KafkaPublisherService(KafkaPublisher publisher, int maxRecordsInPayload, KafkaPublisherContextBuilder kafkaPublisherContextBuilder, KafkaPublisherCallback kafkaPublisherCallback) {
        this.publisher = publisher;
        this.maxRecordsInPayload = maxRecordsInPayload;
        this.kafkaPublisherContextBuilder = kafkaPublisherContextBuilder;
        this.kafkaPublisherCallback = kafkaPublisherCallback;
    }

    public void publishData(SourceContext context, Collection<KafkaPayload> payloads) {
        LOGGER.info("Started publishing data for {} to kafka", context.getSource());
        List<List<KafkaPayload>> payloadData = splitByNumberOfRecordsInPayload(context,payloads);
        payloads.stream().forEach(data -> {
            KafkaPublisherContext publisherContext = getAsyncContext(context, data);
            publisher.publishAsync(publisherContext, kafkaPublisherCallback);
        });
        LOGGER.info("Started publishing data for {} to kafka", context.getSource());
    }

    private List<List<KafkaPayload>> splitByNumberOfRecordsInPayload(SourceContext context, Collection<KafkaPayload> payloads) {
        return Lists.partition(Lists.newArrayList(payloads), maxRecordsInPayload);
    }

    private KafkaPublisherContext getAsyncContext(SourceContext context, KafkaPayload payload) {
        return kafkaPublisherContextBuilder.buildAsyncContext(context.getSource().getPayloadType(), payload);
    }
}
