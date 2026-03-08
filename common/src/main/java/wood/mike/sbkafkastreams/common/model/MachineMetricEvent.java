package wood.mike.sbkafkastreams.common.model;

import java.time.Instant;

public record MachineMetricEvent(
        String machineId,
        String metricType,
        double value,
        Instant timestamp
) {}
