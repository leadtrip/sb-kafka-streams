package wood.mike.sbkafkastreams.producer.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;

@Service
public class MetricProducerService {
    private final KafkaTemplate<String, MachineMetricEvent> kafkaTemplate;

    public MetricProducerService(KafkaTemplate<String, MachineMetricEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMetric(MachineMetricEvent event) {
        // use machineId as the key to ensure all metrics for one machine land in the same partition (crucial for windowing!)
        kafkaTemplate.send("raw-metrics", event.machineId(), event);
    }
}