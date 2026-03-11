package wood.mike.sbkafkastreams.producer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import wood.mike.sbkafkastreams.common.enums.MetricType;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;
import wood.mike.sbkafkastreams.common.model.MachineMetricEventResponse;
import wood.mike.sbkafkastreams.producer.service.MetricGeneratorService;
import wood.mike.sbkafkastreams.producer.service.MetricProducerService;

@RequestMapping("/api")
@RestController
public class MachineMetricController {

    private final MetricProducerService metricProducerService;
    private final MetricGeneratorService generatorService;

    public MachineMetricController(MetricProducerService metricProducerService, MetricGeneratorService generatorService) {
        this.metricProducerService = metricProducerService;
        this.generatorService = generatorService;
    }

    @PostMapping("/metric/add")
    public MachineMetricEventResponse add(@RequestBody MachineMetricEvent machineMetricEvent) {
        metricProducerService.sendMetric(machineMetricEvent);
        return MachineMetricEventResponse.of("success");
    }

    @PostMapping("/generator/run")
    public ResponseEntity<String> generateData(
            @RequestParam(defaultValue = "3") int machines,
            @RequestParam(defaultValue = "100") int iterations) {

        generatorService.generate(machines, iterations);
        return ResponseEntity.ok("Started generating " + (machines * iterations * MetricType.values().length) + " metrics...");
    }
}
