package ru.practicum.collector.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaProperties properties;
    private final Producer<String, SpecificRecordBase> producer;

    public void sendSensorEvent(SensorEventAvro avro) {
        send(properties.getTopics().getSensor(), avro.getHubId(), avro, avro.getTimestamp());
    }

    public void sendHubEvent(HubEventAvro avro) {
        send(properties.getTopics().getHub(), avro.getHubId(), avro, avro.getTimestamp());
    }

    private void send(String topic, String key, SpecificRecordBase value, Instant timestamp) {
        var record = new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send message to topic {} with key {}", topic, key, exception);
            } else {
                log.info("Message sent to topic {} partition {} offset {} with key {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), key);
            }
        });
    }
}
