package com.home.study.elasticsearch.common.service;

import com.home.study.elasticsearch.common.client.SerializableData;
import com.home.study.kafka.data.load.constant.PayloadType;

import java.util.Collection;

public interface ElasticsearchPublisher {
    void publish(PayloadType type, Collection<SerializableData> data);
}
