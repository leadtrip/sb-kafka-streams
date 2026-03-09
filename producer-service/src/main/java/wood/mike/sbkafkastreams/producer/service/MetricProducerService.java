package wood.mike.sbkafkastreams.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class MetricProducerService {
    private final KafkaTemplate<String, MachineMetricEvent> kafkaTemplate;

    public MetricProducerService(KafkaTemplate<String, MachineMetricEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMetric(MachineMetricEvent event) {
        CompletableFuture<SendResult<String, MachineMetricEvent>> future = kafkaTemplate.send("raw-metrics", event.machineId(), event);
        future.whenComplete((result, ex) -> {
            if(ex != null) {
                log.error("Metric event for machine {} send failed", event.machineId(), ex);
            }
            else {
                log.info("Metric event for machine {} completed",  event.machineId());
            }
        });
    }
}