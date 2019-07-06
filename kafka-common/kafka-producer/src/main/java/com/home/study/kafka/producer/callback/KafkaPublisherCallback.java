package com.home.study.kafka.producer.callback;

import com.home.luv.kafka.common.data.KafkaPayload;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class KafkaPublisherCallback implements ListenableFutureCallback<SendResult<String, KafkaPayload>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisherCallback.class);

    @Override
    public void onFailure(Throwable throwable) {
        LOGGER.error("Error on publishing message :" , throwable);
    }

    @Override
    public void onSuccess(SendResult<String, KafkaPayload> result) {
        RecordMetadata metaData = result.getRecordMetadata();
        LOGGER.info("topic = {} , partition = {} ,  offset ={} ", metaData.topic(), metaData.partition(), metaData.offset());
    }
}
