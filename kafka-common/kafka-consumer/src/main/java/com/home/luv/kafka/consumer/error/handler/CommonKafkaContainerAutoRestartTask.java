package com.home.luv.kafka.consumer.error.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CommonKafkaContainerAutoRestartTask implements Runnable {

    private static Logger LOGGER  = LoggerFactory.getLogger(CommonKafkaContainerAutoRestartTask.class);
    private final KafkaListenerEndpointRegistry registry;
    private final Set<String> stoppedContainer;
    private final Map<String, Integer> containerRestartCount;
    private final int maxRetryCount;


    CommonKafkaContainerAutoRestartTask(KafkaListenerEndpointRegistry registry, Set<String> stoppedContainers, Map<String, Integer> containerRestartCount, int maxRetryCount) {
        this.registry = registry;
        this.stoppedContainer = stoppedContainers;
        this.containerRestartCount = containerRestartCount;
        this.maxRetryCount = maxRetryCount;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Running KafkaContainerAutoRestartTask.. trying to restart {} containers {}", stoppedContainer.size(), stoppedContainer);
            synchronized (com.home.luv.kafka.consumer.error.handler.KafkaContainerAutoRestartHandler.class) {
                Set<String> containersRestarted = new HashSet<>();
                stoppedContainer.stream().filter(this::shouldAttemptRestart).forEach(containerId -> restartContainer(containerId, containersRestarted));
            }
        } catch(Throwable th) {
            LOGGER.error("Problem while trying to restart {}" + stoppedContainer, th);
        }
    }

    private void restartContainer(String containerId, Set<String> containersRestarted) {
        registry.getListenerContainer(containerId).start();
        containersRestarted.add(containerId);
        incrementRestartCount(containerId);
        LOGGER.info("Started {}" ,containerId);
    }

    private void incrementRestartCount(String containerId) {
        Integer retryCount = containerRestartCount.get(containerId);
        containerRestartCount.put(containerId, containerRestartCount.get(containerId) == null ? 0 : ++retryCount);
    }

    private boolean shouldAttemptRestart(String containerId) {
        return shouldTryRestartingAgain(containerId) && registry.getListenerContainer(containerId) != null && !registry.getListenerContainer(containerId).isRunning();
    }

    private boolean shouldTryRestartingAgain(String containerId) {

        boolean shouldRestart = containerRestartCount.containsKey(containerId) || containerRestartCount.get(containerId) < maxRetryCount;

        if(!shouldRestart) {
            LOGGER.error("Container {} has reached max retries limit {}", containerId, maxRetryCount);
        }
        return shouldRestart;
    }
}
