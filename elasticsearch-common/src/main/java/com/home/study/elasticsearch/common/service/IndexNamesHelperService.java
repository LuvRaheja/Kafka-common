package com.home.study.elasticsearch.common.service;

import com.google.common.collect.Sets;
import com.home.study.kafka.data.load.constant.PayloadType;

import java.util.Set;

public class IndexNamesHelperService {
    public Set<String> getIndicesForSave(PayloadType type) {
        return Sets.newHashSet(type.name());
    }
}
