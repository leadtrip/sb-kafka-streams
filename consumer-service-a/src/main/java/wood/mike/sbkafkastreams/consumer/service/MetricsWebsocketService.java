package wood.mike.sbkafkastreams.consumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import wood.mike.sbkafkastreams.common.model.MetricUpdate;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricsWebsocketService {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "machine-metric-averages", groupId = "dashboard-bridge-group")
    public void consume(String rawJson) {
        try {
            MetricUpdate event = objectMapper.readValue(rawJson, MetricUpdate.class);
            log.info("Received and deserialized: {}", event);
            messagingTemplate.convertAndSend("/topic/averages", event);
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", rawJson, e);
        }
    }
}
