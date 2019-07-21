package com.home.study.elasticsearch.common.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    private final String elasticsearchHost;

    private final int port;

    private final String clusterName;

    public ElasticsearchConfig(@Value("${elasticsearch.host:localhost}") String elasticsearchHost,
                               @Value("${elasticsearch.port:9200}")int port,
                               @Value("${elasticsearch.clusterName:elasticsearch}")String clusterName){

        this.elasticsearchHost = elasticsearchHost;
        this.port = port;
        this.clusterName = clusterName;
    }

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(elasticsearchHost, port, "http"),
                        new HttpHost(elasticsearchHost, 9201, "http")));
    }


}
