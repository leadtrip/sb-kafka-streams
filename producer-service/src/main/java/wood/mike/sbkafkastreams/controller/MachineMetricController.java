package wood.mike.sbkafkastreams.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;
import wood.mike.sbkafkastreams.common.model.MachineMetricEventResponse;
import wood.mike.sbkafkastreams.producer.service.MetricProducerService;

@RestController
public class MachineMetricController {

    private final MetricProducerService metricProducerService;

    public MachineMetricController(MetricProducerService metricProducerService) {
        this.metricProducerService = metricProducerService;
    }

    @PostMapping("/api/metric/add")
    public MachineMetricEventResponse add(@RequestBody MachineMetricEvent machineMetricEvent) {
        metricProducerService.sendMetric(machineMetricEvent);
        return MachineMetricEventResponse.of("success");
    }
}
