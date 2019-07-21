package com.home.study.elasticsearch.common.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.home.study.elasticsearch.common.client.SerializableData;
import com.home.study.elasticsearch.common.template.ElasticsearchTemplate;
import com.home.study.kafka.data.load.constant.PayloadType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ElasticsearchPublisherService implements ElasticsearchPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchPublisherService.class);
    private final IndexNamesHelperService indexNamesHelperService;
    private final Map<String, Boolean> indexStatusMap = new ConcurrentHashMap<>();
    private final ElasticsearchTemplate elasticsearchTemplate;

    public ElasticsearchPublisherService(IndexNamesHelperService indexNamesHelperService, ElasticsearchTemplate elasticsearchTemplate) {
        this.indexNamesHelperService = indexNamesHelperService;
        this.elasticsearchTemplate = elasticsearchTemplate;
    }

    @Override
    public void publish(PayloadType type, Collection<SerializableData> data) {
        LOGGER.info("Publishing {} records of type: {}", data.size(), type);
        try {
            if (!CollectionUtils.isEmpty(data)) {
                publishData(type, data);
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurred while publishing data ", e);
            throw new RuntimeException();
        }
        LOGGER.info("Completed Publishing {} records of type: {}", data.size(), type);
    }

    private void publishData(PayloadType type, Collection<SerializableData> data) {
        Set<String> indexNames = indexNamesHelperService.getIndicesForSave(type);
        indexNames.forEach(indexName -> {
            createIndexIfItDoesnotExist(indexName);
            createAnndExecuteBulkRequest(type, indexName, data);
        });

    }

    private void createIndexIfItDoesnotExist(String indexName) {
        if (!indexStatusMap.containsKey(indexName)) {
            indexStatusMap.put(indexName, elasticsearchTemplate.createIndex(indexName));
        }
    }


    private void createAnndExecuteBulkRequest(PayloadType type, String indexName, Collection<SerializableData> data) {

        BulkRequest request = new BulkRequest();
        data.forEach(record -> request.add(getUpsertRequest(type, indexName, record)));
        elasticsearchTemplate.executeBulkRequest(request);
    }

    private UpdateRequest getUpsertRequest(PayloadType type, String indexName, SerializableData toIndex) {
        byte[] toIndexIfNotExist = getData(toIndex);
        String id = toIndex.getId();
        IndexRequest indexRequest = getIndexRequest(type, indexName, id);
        indexRequest.source(toIndexIfNotExist);
        UpdateRequest updateRequest = getUpdateRequest(type, indexName, id);
        updateRequest.doc(toIndexIfNotExist);
        updateRequest.upsert(indexRequest);
        return updateRequest;
    }

    private UpdateRequest getUpdateRequest(PayloadType type, String indexName, String id) {
        return new UpdateRequest(indexName, type.name(), id);
    }

    private IndexRequest getIndexRequest(PayloadType type, String indexName, String id) {
        return new IndexRequest(indexName, type.name(), id);
    }

    private byte[] getData(SerializableData toIndex) {
        try {
            return new ObjectMapper().writeValueAsBytes(toIndex);
        } catch (JsonProcessingException e) {
            LOGGER.error("Problem getting bytes of data {}", toIndex, e);
        }
        return null;
    }
}
