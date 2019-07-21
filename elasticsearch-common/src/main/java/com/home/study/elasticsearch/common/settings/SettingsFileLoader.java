package com.home.study.elasticsearch.common.settings;

import io.micrometer.core.instrument.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Objects;

@Component
public class SettingsFileLoader {
    public static final Logger LOGGER = LoggerFactory.getLogger(SettingsFileLoader.class);
    private static final String SETTINGS_FILE_NAME = "mapping/%s-settings.json";
    private static final String SETTINGS_DEFAULT_FILE_NAME = "mapping/elasticsearch-settings.json";

    public String getIndexSettingsIfExists(String indexName) {
        String fileName = getSettingsFileName(indexName);
        try {
            InputStream stream = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (Objects.isNull(stream)) {
                stream = this.getClass().getClassLoader().getResourceAsStream(SETTINGS_DEFAULT_FILE_NAME);
                if (Objects.isNull(stream)) {
                    LOGGER.info("Returning wiithout finding file name for index name {} and type {} ", indexName);
                    return null;
                }
            }
            return IOUtils.toString(stream, Charset.defaultCharset());
        } catch (Exception e) {
            LOGGER.error("Error loading file ", e);
        }
        return null;
    }

    private String getSettingsFileName(String indexName) {
        return String.format(SETTINGS_FILE_NAME, indexName);
    }

}
