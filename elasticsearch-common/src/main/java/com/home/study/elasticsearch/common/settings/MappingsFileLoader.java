package com.home.study.elasticsearch.common.settings;

import io.micrometer.core.instrument.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Objects;

@Component
public class MappingsFileLoader {
    public static final Logger LOGGER = LoggerFactory.getLogger(MappingsFileLoader.class);
    private static final String MAPPING_FILE_NAME = "mapping/%s-%s-mapping.json";

    public String getMappingsIfExists(String indexName, String type) {
        String fileName =  getMappingFileName(indexName, type);
        try {
            InputStream stream = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if(Objects.isNull(stream)) {
                LOGGER.info("Returning wiithout finding file name for index name {} and type {} ", indexName, type);
                return null;
            }
            String mappings = IOUtils.toString(stream, Charset.defaultCharset());
            if(Objects.nonNull(mappings)) {
                mappings = replacePlaceHolders(mappings, type);
                LOGGER.info("Mappings Set for index name {} and type {}", indexName, type);
            }
            return mappings;
        } catch (Exception e) {
            LOGGER.error("Error loading file ", e);
        }
        return null;

    }

    private String replacePlaceHolders(String mappings, String type) {
        return mappings.replaceAll("<INDEX_TYPE>", type);
    }

    private String getMappingFileName(String indexName, String type) {
        return String.format(MAPPING_FILE_NAME, indexName, type);
    }
}
