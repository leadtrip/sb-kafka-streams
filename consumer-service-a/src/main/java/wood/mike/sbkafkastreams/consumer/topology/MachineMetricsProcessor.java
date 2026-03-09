package wood.mike.sbkafkastreams.consumer.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;

import java.time.Duration;

@Slf4j
@Component
public class MachineMetricsProcessor {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        var doubleSerde = Serdes.Double();
        var eventSerde = new JacksonJsonSerde<>(MachineMetricEvent.class);

        KStream<String, MachineMetricEvent> metrics = streamsBuilder.stream(
                "raw-metrics",
                Consumed.with(Serdes.String(), eventSerde)
        );

        metrics
                .filter((key, event) -> event.value() >= 0)
                .groupBy((key, event) -> event.machineId() + ":" + event.metricType(),
                        Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(
                        () -> 0.0,
                        (key, event, aggregate) -> (aggregate + event.value()) / 2,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("metrics-avg-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(doubleSerde)
                )
                .toStream()
                .peek((windowedKey, avg) -> {
                    String[] parts = windowedKey.key().split(":");
                    log.info("Machine: {} | Metric: {} | Avg: {}", parts[0], parts[1], avg);
                })
                .to("machine-metric-averages", Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, 60000L),
                        doubleSerde));
    }
}