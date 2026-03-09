package wood.mike.sbkafkastreams.consumer.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;
import wood.mike.sbkafkastreams.common.model.MachineMetricEvent;

@Slf4j
@Component
public class MachineMetricsProcessor {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, MachineMetricEvent> metrics = streamsBuilder.stream(
                "raw-metrics",
                Consumed.with(Serdes.String(), new JacksonJsonSerde<>(MachineMetricEvent.class))
        );

        metrics
                //.peek((key, event) -> log.info("Received: {}", event))
                .filter((key, event) -> event.value() >= 0)
                .to("processed-metrics");
    }
}