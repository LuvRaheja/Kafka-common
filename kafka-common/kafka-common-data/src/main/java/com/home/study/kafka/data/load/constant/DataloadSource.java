package com.home.study.kafka.data.load.constant;

public enum DataloadSource {

    TWITTER(com.home.study.kafka.data.load.constant.PayloadType.TWITTER),
    TESTING(com.home.study.kafka.data.load.constant.PayloadType.TESTING);

    private final com.home.study.kafka.data.load.constant.PayloadType payloadType;

    DataloadSource(com.home.study.kafka.data.load.constant.PayloadType payloadType) {
        this.payloadType = payloadType;
    }

    public com.home.study.kafka.data.load.constant.PayloadType getPayloadType() {
        return payloadType;
    }
}
