package com.home.study.elasticsearch.common.service;

import com.google.common.util.concurrent.Striped;
import com.home.study.elasticsearch.common.AutocloseableStripedLock;
import com.home.study.elasticsearch.common.settings.MappingsFileLoader;
import com.home.study.elasticsearch.common.settings.SettingsFileLoader;
import com.home.study.elasticsearch.common.template.ElasticsearchTemplate;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;

public class ElasticsearchIndexService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIndexService.class);
    private Striped<ReadWriteLock> createIndexLocks;
    private final SettingsFileLoader settingsFileLoader;
    private final ElasticsearchTemplate elasticsearchTemplate;
    private final MappingsFileLoader mappingsFileLoader;

    public ElasticsearchIndexService(SettingsFileLoader settingsFileLoader, ElasticsearchTemplate elasticsearchTemplate, MappingsFileLoader mappingsFileLoader) {
        this.settingsFileLoader = settingsFileLoader;
        this.elasticsearchTemplate = elasticsearchTemplate;
        this.mappingsFileLoader = mappingsFileLoader;
        this.createIndexLocks = Striped.readWriteLock(10);
    }

    public boolean createIndex(String indexName) {
        try (AutocloseableStripedLock lock = AutocloseableStripedLock.getLock(createIndexLocks, indexName)) {
            if (indexExists(indexName)) {
                return true;
            }
            String settings = settingsFileLoader.getIndexSettingsIfExists(indexName);
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder().loadFromSource(settings, XContentType.JSON));
            addMappings(request, indexName);
            return elasticsearchTemplate.execute(request);
        } catch (InterruptedException e) {
            LOGGER.error("Problem creating index for : {}", indexName, e);
        } catch (Exception e) {
            LOGGER.error("Problem creating index for : {}", indexName, e);
        }
        return false;
    }

    private void addMappings(CreateIndexRequest request, String indexName) {
        List<String> indexTypesToIndex = Collections.singletonList(indexName);
        indexTypesToIndex.forEach(type -> addMappingForType(request, indexName, type));
    }

    private void addMappingForType(CreateIndexRequest request, String indexName, String type) {
        String mapping = mappingsFileLoader.getMappingsIfExists(indexName, type);
        if (Objects.nonNull(mapping)) {
            request.mapping(mapping, XContentType.JSON);
        }
    }

    private boolean indexExists(String indexName) {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return elasticsearchTemplate.execute(request);
    }
}
