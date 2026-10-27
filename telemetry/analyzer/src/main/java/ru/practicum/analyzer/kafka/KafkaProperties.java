package ru.practicum.analyzer.kafka;

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

    @Getter
    @Setter
    public static class Topics {
        private String hubs;
        private String snapshots;
    }

    @Getter
    @Setter
    public static class Consumer {
        private ConsumerConfig snapshot = new ConsumerConfig();
        private ConsumerConfig hub = new ConsumerConfig();
    }

    @Getter
    @Setter
    public static class ConsumerConfig {
        private String groupId;
        private Class<?> keyDeserializer;
        private Class<?> valueDeserializer;
        private String autoOffsetReset;
        private boolean enableAutoCommit;
        private int maxPollRecords;
    }
}