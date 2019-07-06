package com.home.luv.kafka.consumer.error.handler;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaContainerAutoRestartHandler {


    private final Set<String> stoppedContainers;

    public KafkaContainerAutoRestartHandler(KafkaListenerEndpointRegistry registry,
                                            @Value("${kafka.container.autorestart.initial.delay.in.secs:60}") int initialDelay,
                                            @Value("${kafka.container.autorestart.run.interval.in.secs:60}") int timeInterval,
                                            @Value("${kafka.container.autorestart.max.retry.count:5}") int maxRetryCount) {
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.stoppedContainers = new HashSet<>();
        Map<String, Integer> containerRestartCount = new HashMap<>();
        scheduledExecutor.scheduleWithFixedDelay(new CommonKafkaContainerAutoRestartTask(registry, stoppedContainers, containerRestartCount, maxRetryCount),
                initialDelay, timeInterval, TimeUnit.SECONDS);
    }

    public void addStoppedContainer(String containerId) {
        if(stoppedContainers.contains(containerId)) {
            return;
        }
        synchronized (KafkaContainerAutoRestartHandler.class) {
            stoppedContainers.add(containerId);
        }
    }
}
