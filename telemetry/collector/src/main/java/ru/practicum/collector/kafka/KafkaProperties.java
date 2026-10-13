package ru.practicum.collector.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.yandex.practicum.kafka.serializer.BaseAvroSerializer;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private Topics topics = new Topics();
    private Producer producer = new Producer();

    @Getter
    @Setter
    public static class Topics {
        private String sensor = "telemetry.sensors.v1";
        private String hub = "telemetry.hubs.v1";
    }

    @Getter
    @Setter
    public static class Producer {
        private String acks = "all";
        private boolean enableIdempotence = true;
        private Class<?> keySerializer = StringSerializer.class;
        private Class<?> valueSerializer = BaseAvroSerializer.class;
    }
}