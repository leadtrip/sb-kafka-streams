package wood.mike.sbkafkastreams.common.model;

import java.time.Instant;

public record MachineMetricEvent(
        String machineId,
        String metricType,
        Double value,
        Instant timestamp
) {}
