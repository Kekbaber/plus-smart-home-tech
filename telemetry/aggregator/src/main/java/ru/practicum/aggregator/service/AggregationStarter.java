package ru.practicum.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.aggregator.kafka.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private static final int COMMIT_BATCH_SIZE = 10;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final KafkaProperties kafkaProperties;
    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SensorsSnapshotAvro> producer;
    private final SnapshotService snapshotService;

    public void start() {
        String sensorsTopic = kafkaProperties.getTopics().getSensors();
        String snapshotsTopic = kafkaProperties.getTopics().getSnapshots();

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        consumer.subscribe(List.of(sensorsTopic));
        log.info("Aggregator subscribed to topic {}", sensorsTopic);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        int processedRecords = 0;

        try {
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    if (event != null) {
                        snapshotService.updateState(event)
                                .ifPresent(snapshot -> sendSnapshot(snapshotsTopic, snapshot));
                    }

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    if (++processedRecords % COMMIT_BATCH_SIZE == 0) {
                        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                            if (exception != null) {
                                log.warn("Failed to commit offsets: {}", offsets, exception);
                            }
                        });
                    }
                }
            }
        } catch (WakeupException ignored) {
            log.info("Wakeup received, stopping aggregator");
        } catch (Exception e) {
            log.error("Unexpected error while processing sensor events", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync(currentOffsets);
                log.info("Final offsets committed: {}", currentOffsets);
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }

    private void sendSnapshot(String topic, SensorsSnapshotAvro snapshot) {
        ProducerRecord<String, SensorsSnapshotAvro> message = new ProducerRecord<>(
                topic,
                null,
                snapshot.getTimestamp().toEpochMilli(),
                snapshot.getHubId(),
                snapshot);

        producer.send(message, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send snapshot for hub {} to topic {}",
                        snapshot.getHubId(), topic, exception);
            } else {
                log.debug("Snapshot for hub {} sent to topic {} partition {} offset {}",
                        snapshot.getHubId(), metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
}