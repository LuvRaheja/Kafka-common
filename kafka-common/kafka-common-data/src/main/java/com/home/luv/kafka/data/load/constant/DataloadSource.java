package com.home.luv.kafka.data.load.constant;

public enum DataloadSource {

    TWITTER(PayloadType.TWITTER),
    TESTING(PayloadType.TESTING);

    private final PayloadType payloadType;

    DataloadSource(PayloadType payloadType) {
        this.payloadType = payloadType;
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }
}
