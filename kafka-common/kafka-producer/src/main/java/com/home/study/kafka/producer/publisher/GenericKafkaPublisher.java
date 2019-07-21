package com.home.study.kafka.producer.publisher;

import com.home.study.kafka.common.data.KafkaPayload;
import com.home.study.kafka.common.data.KafkaPublisherContext;
import com.home.study.kafka.producer.callback.KafkaPublisherCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

public class GenericKafkaPublisher implements KafkaPublisher {

    private static final Logger LOGGEr  = LoggerFactory.getLogger(GenericKafkaPublisher.class);
    private final KafkaTemplate<String, KafkaPayload> kafkaTemplate;

    public GenericKafkaPublisher(KafkaTemplate<String, KafkaPayload> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public boolean publish(KafkaPublisherContext context) {
        try {
            KafkaPayload payload = context.getPayload();
            SendResult<String, KafkaPayload> sendResult = publishAndGetResult(context,payload);
            printRecordMetaData(payload, sendResult);
            return true;
        } catch (Exception e) {
            throw new RuntimeException();
        }

    }

    private void printRecordMetaData(KafkaPayload payload, SendResult<String, KafkaPayload> sendResult) {
    }

    private SendResult<String, KafkaPayload> publishAndGetResult(KafkaPublisherContext context, KafkaPayload payload) throws ExecutionException, InterruptedException {

        return kafkaTemplate.send(context.getTopic(), payload.getMessageId(), payload).get();
    }

    @Override
    public boolean publishAsync(KafkaPublisherContext context, KafkaPublisherCallback callback) {
        KafkaPayload payload = context.getPayload();
        ListenableFuture<SendResult<String, KafkaPayload>> listenableFuture = kafkaTemplate.send(context.getTopic(), payload.getMessageId(), payload);
        listenableFuture.addCallback(callback);
        return false;
    }
}
