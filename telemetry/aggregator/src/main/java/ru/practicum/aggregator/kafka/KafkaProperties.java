package ru.practicum.aggregator.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private String bootstrapServers;
    private Topics topics = new Topics();
    private Consumer consumer = new Consumer();
    private Producer producer = new Producer();

    @Getter
    @Setter
    public static class Topics {
        private String sensors;
        private String snapshots;
    }

    @Getter
    @Setter
    public static class Consumer {
        private String groupId;
        private Class<?> keyDeserializer;
        private Class<?> valueDeserializer;
        private String autoOffsetReset;
        private boolean enableAutoCommit;
        private int commitBatchSize;
        private long pollTimeoutMs;
    }

    @Getter
    @Setter
    public static class Producer {
        private Class<?> keySerializer;
        private Class<?> valueSerializer;
        private String acks;
        private boolean enableIdempotence;
    }
}
