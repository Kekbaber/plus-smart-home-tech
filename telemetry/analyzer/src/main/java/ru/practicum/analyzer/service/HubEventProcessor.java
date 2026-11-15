package ru.practicum.analyzer.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import ru.practicum.analyzer.kafka.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Component
public class HubEventProcessor extends AbstractEventProcessor<HubEventAvro> {

    private final KafkaProperties kafkaProperties;
    private final HubEventService hubEventService;

    public HubEventProcessor(
            KafkaProperties kafkaProperties,
            HubEventService hubEventService,
            @Qualifier("hubEventConsumerFactory") ConsumerFactory<String, HubEventAvro> consumerFactory) {
        super(consumerFactory);
        this.kafkaProperties = kafkaProperties;
        this.hubEventService = hubEventService;
    }

    @Override
    protected String getTopic() {
        return kafkaProperties.getTopics().getHubs();
    }

    @Override
    protected void handle(HubEventAvro event) {
        hubEventService.handle(event);
    }
}
