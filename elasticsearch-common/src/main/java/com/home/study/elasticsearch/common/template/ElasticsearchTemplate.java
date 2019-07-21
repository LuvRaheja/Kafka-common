package com.home.study.elasticsearch.common.template;

import com.home.study.elasticsearch.common.callback.ElasticsearchBulkResponseActionListener;
import com.home.study.elasticsearch.common.service.ElasticsearchIndexService;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ElasticsearchTemplate {
    private final ElasticsearchIndexService elasticsearchIndexService;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTemplate.class);
    private final RestHighLevelClient client;
    private ElasticsearchBulkResponseActionListener<BulkResponse> esBulkListener;

    public ElasticsearchTemplate(ElasticsearchIndexService elasticsearchIndexService, RestHighLevelClient client) {
        this.elasticsearchIndexService = elasticsearchIndexService;
        this.client = client;
    }

    public Boolean createIndex(String indexName) {
        try {
            boolean indexCreated = elasticsearchIndexService.createIndex(indexName);
            LOGGER.info("Index Created :{}", indexCreated);
            return indexCreated;
        } catch(Exception e) {
            throw new RuntimeException("Problem creating elasticsearch index : " +  indexName, e);
        }
    }

    public void executeBulkRequest(BulkRequest request) {
        client.bulkAsync(request, RequestOptions.DEFAULT, esBulkListener);
    }

    public boolean execute(CreateIndexRequest request) {
        try {
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            LOGGER.error("Problem in creating index {}", request, e);
        }
        return false;
    }

    public boolean execute(GetIndexRequest request) {
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOGGER.error("Problem in checking index status for index {}", request, e);
        }
        return false;
    }
}
