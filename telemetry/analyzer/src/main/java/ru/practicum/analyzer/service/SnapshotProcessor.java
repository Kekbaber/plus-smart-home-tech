package ru.practicum.analyzer.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.practicum.analyzer.kafka.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Component
public class SnapshotProcessor extends AbstractEventProcessor<SensorsSnapshotAvro> {

    private final KafkaProperties kafkaProperties;
    private final ScenarioAnalyzerService scenarioAnalyzerService;

    public SnapshotProcessor(
            KafkaProperties kafkaProperties,
            ScenarioAnalyzerService scenarioAnalyzerService,
            @Qualifier("snapshotConsumer") Consumer<String, SensorsSnapshotAvro> consumer) {
        super(consumer);
        this.kafkaProperties = kafkaProperties;
        this.scenarioAnalyzerService = scenarioAnalyzerService;
    }

    @Override
    protected String getTopic() {
        return kafkaProperties.getTopics().getSnapshots();
    }

    @Override
    protected void handle(SensorsSnapshotAvro snapshot) {
        scenarioAnalyzerService.analyze(snapshot);
    }
}
