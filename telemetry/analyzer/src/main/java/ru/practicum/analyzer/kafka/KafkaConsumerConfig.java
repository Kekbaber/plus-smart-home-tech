package ru.practicum.analyzer.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    private Properties props(KafkaProperties.ConsumerConfig cfg) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, cfg.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, cfg.getValueDeserializer());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cfg.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, cfg.isEnableAutoCommit());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cfg.getMaxPollRecords());
        return props;
    }

    @Bean(destroyMethod = "")
    public Consumer<String, HubEventAvro> hubEventConsumer() {
        return new KafkaConsumer<>(props(kafkaProperties.getConsumer().getHub()));
    }

    @Bean(destroyMethod = "")
    public Consumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        return new KafkaConsumer<>(props(kafkaProperties.getConsumer().getSnapshot()));
    }
}
