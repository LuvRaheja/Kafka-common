package com.home.study.kafka.application;

import com.home.study.kafka.common.data.SimpleKafkaPayload;
import com.home.study.kafka.data.load.constant.DataloadSource;
import com.home.study.kafka.data.load.constant.SourceContext;
import com.home.study.kafka.producer.publisher.KafkaPublisherService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;

@RestController
public class SampleRestController implements SourceContext {

    private final KafkaPublisherService kafkaPublisherService;

    SampleRestController(KafkaPublisherService kafkaPublisherService) {
        this.kafkaPublisherService = kafkaPublisherService;
    }


    @RequestMapping("/sendMessage/{message}")
    public String sendMessage (@PathVariable("message") String message) {
        kafkaPublisherService.publishData(this::getSource, Collections.singletonList(new SimpleKafkaPayload(message)));
        return "messageSent";
    }

    @Override
    public DataloadSource getSource() {
        return DataloadSource.TESTING;
    }
}
