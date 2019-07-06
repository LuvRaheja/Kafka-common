 package com.home.study.kafka.config;

 import com.home.luv.kafka.consumer.error.handler.KafkaContainerAutoRestartHandler;
 import com.home.study.kafka.consumer.config.AbstractKafkaConsumerConfig;
 import org.apache.kafka.common.serialization.Deserializer;
 import org.apache.kafka.common.serialization.StringDeserializer;
 import org.springframework.beans.factory.annotation.Autowired;
 import org.springframework.beans.factory.annotation.Value;
 import org.springframework.context.annotation.Bean;
 import org.springframework.kafka.annotation.EnableKafka;
 import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
 import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

 @EnableKafka
 public class CommonKafkaConsumerConfig extends AbstractKafkaConsumerConfig<String> {

     @Value("${kafka.bootstrap.server:localhost:9092}")
     String bootstrapAddress;

     @Value("${kafka.topicName.max.poll.config:1000}")
     int maxPollConfig;

     @Value("${kafka.topicName.max.poll.interval:1000}")
     int maxPollIntervalInMillis;

     @Autowired
     KafkaListenerEndpointRegistry registry;

     @Autowired
     private KafkaContainerAutoRestartHandler autoRestartHandler;


     @Bean
     ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(@Value("${kafka.topic.name:topicName}") String topic,
                                                                                           @Value("${kafka.topic.name.partitionsCount:1}") int partitions) {
         return getConcurrentKafkaListenerContainerFactory(bootstrapAddress, topic+".group", maxPollConfig, maxPollIntervalInMillis, partitions,
                 getErrorHandler(registry, topic, autoRestartHandler), true);
     }


     @Override
     public Deserializer<String> payloadJsonDeserializer() {
         return new StringDeserializer();
     }
 }
