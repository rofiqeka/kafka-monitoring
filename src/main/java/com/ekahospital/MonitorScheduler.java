package com.ekahospital;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MonitorScheduler {

    @Inject
    KafkaMonitorService kafkaMonitorService;

    @Scheduled(every = "60s") // Adjust the interval as needed
    void monitorKafkaConsumers() {
        kafkaMonitorService.checkConsumerLag();
    }
}
