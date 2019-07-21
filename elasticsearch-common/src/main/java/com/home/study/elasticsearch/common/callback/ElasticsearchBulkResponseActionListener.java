package com.home.study.elasticsearch.common.callback;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchBulkResponseActionListener<T> implements ActionListener<BulkResponse> {

    private final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchBulkResponseActionListener.class);
    @Override
    public void onResponse(BulkResponse bulkItemResponse) {
        LOGGER.info("Successfully executed Bulk request in {} seconds ", bulkItemResponse.getTook().seconds());
    }

    @Override
    public void onFailure(Exception e) {
        LOGGER.error("Error in executing Bulk request. Check error ", e);
    }
}
