package ru.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractEventProcessor<T> implements Runnable {

    /**
     * Стратегия коммита оффсетов:
     * SYNC_PER_BATCH — один commitSync после обработки всей пачки (для редких событий);
     * ASYNC_BATCHED — commitAsync каждые commitBatchSize сообщений + commitSync в finally (для частых событий).
     */
    protected enum CommitMode {
        SYNC_PER_BATCH,
        ASYNC_BATCHED
    }

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final Consumer<String, T> consumer;

    protected AbstractEventProcessor(ConsumerFactory<String, T> consumerFactory) {
        this.consumer = consumerFactory.createConsumer();
    }

    protected abstract String getTopic();

    protected abstract void handle(T event);

    protected CommitMode commitMode() {
        return CommitMode.SYNC_PER_BATCH;
    }

    protected int commitBatchSize() {
        return 1;
    }

    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void run() {
        String topic = getTopic();
        String processorName = getClass().getSimpleName();
        CommitMode commitMode = commitMode();
        int commitBatchSize = commitBatchSize();

        consumer.subscribe(List.of(topic));
        log.info("{} subscribed to topic {}", processorName, topic);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        int processedRecords = 0;

        try {
            while (true) {
                try {
                    ConsumerRecords<String, T> records = consumer.poll(POLL_TIMEOUT);

                    for (ConsumerRecord<String, T> record : records) {
                        if (record.value() != null) {
                            try {
                                handle(record.value());
                            } catch (RuntimeException e) {
                                log.error("Failed to process record {}-{} at offset {}",
                                        record.topic(), record.partition(), record.offset(), e);
                                throw e;
                            }
                        }

                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));

                        if (commitMode == CommitMode.ASYNC_BATCHED
                                && ++processedRecords % commitBatchSize == 0) {
                            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                                if (exception != null) {
                                    log.warn("{} failed to commit offsets: {}", processorName, offsets, exception);
                                }
                            });
                        }
                    }

                    if (commitMode == CommitMode.SYNC_PER_BATCH && !records.isEmpty()) {
                        consumer.commitSync(currentOffsets);
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
                consumer.commitSync(currentOffsets);
                log.info("{} final offsets committed: {}", processorName, currentOffsets);
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
}
