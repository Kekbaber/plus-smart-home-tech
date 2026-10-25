package ru.practicum.aggregator.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.yandex.practicum.kafka.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.serializer.BaseAvroSerializer;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private Topics topics = new Topics();
    private Consumer consumer = new Consumer();
    private Producer producer = new Producer();

    @Getter
    @Setter
    public static class Topics {
        private String sensors = "telemetry.sensors.v1";
        private String snapshots = "telemetry.snapshots.v1";
    }

    @Getter
    @Setter
    public static class Consumer {
        private String groupId = "telemetry.aggregator";
        private Class<?> keyDeserializer = StringDeserializer.class;
        private Class<?> valueDeserializer = SensorEventDeserializer.class;
        private String autoOffsetReset = "earliest";
        private boolean enableAutoCommit = false;
    }

    @Getter
    @Setter
    public static class Producer {
        private Class<?> keySerializer = StringSerializer.class;
        private Class<?> valueSerializer = BaseAvroSerializer.class;
        private String acks = "all";
        private boolean enableIdempotence = true;
    }
}