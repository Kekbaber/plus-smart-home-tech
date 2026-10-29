package ru.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractEventProcessor<T> implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final Consumer<String, T> consumer;

    protected AbstractEventProcessor(Consumer<String, T> consumer) {
        this.consumer = consumer;
    }

    protected abstract String getTopic();

    protected abstract void handle(T event);

    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void run() {
        String topic = getTopic();
        String processorName = getClass().getSimpleName();

        consumer.subscribe(List.of(topic));
        log.info("{} subscribed to topic {}", processorName, topic);

        try {
            while (true) {
                try {
                    ConsumerRecords<String, T> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, T> record : records) {
                        if (record.value() != null) {
                            try {
                                handle(record.value());
                            } catch (Exception e) {
                                log.error("Failed to process record from {}-{} at offset {}",
                                        record.topic(), record.partition(), record.offset(), e);
                            }
                        }

                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
                    }
                } catch (RecordDeserializationException e) {
                    TopicPartition tp = new TopicPartition(e.topicPartition().topic(), e.topicPartition().partition());
                    consumer.seek(tp, e.offset() + 1);
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(e.offset() + 1)));
                    log.error("Skipping undeserializable record {}-{} offset {}", e.topicPartition().topic(),
                            e.topicPartition().partition(), e.offset(), e);
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup received, stopping {}", processorName);
        } catch (Exception e) {
            log.error("{} stopped due to unexpected error", processorName, e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Error while closing consumer", e);
            }
            log.info("{} stopped", processorName);
        }
    }
}
