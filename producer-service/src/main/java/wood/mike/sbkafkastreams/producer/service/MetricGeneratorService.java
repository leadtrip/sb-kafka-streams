package wood.mike.sbkafkastreams.producer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import wood.mike.sbkafkastreams.common.enums.MetricType;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;

import java.time.Instant;
import java.util.Random;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricGeneratorService {

    private static final int SLEEP_BETWEEN_METRIC_GENERATION = 5000;

    private final MetricProducerService metricProducerService;

    @Async
    public void generate(int machineCount, int iterations) {
        Random random = new Random();

        for (int i = 0; i < iterations; i++) {
            for (int m = 1; m <= machineCount; m++) {
                String machineId = String.format("server-rack-%02d", m);

                for (MetricType metricType : MetricType.values()) {
                    double value = 10 + (random.nextDouble() * 80);
                    MachineMetricEvent event = new MachineMetricEvent(
                            machineId,
                            metricType.name(),
                            value,
                            Instant.now()
                    );

                    metricProducerService.sendMetric(event);
                }
            }

            try { Thread.sleep(SLEEP_BETWEEN_METRIC_GENERATION); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        log.info("Generation task complete.");
    }
}