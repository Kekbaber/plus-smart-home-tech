package ru.practicum.analyzer.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.ConsumerFactory;
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
            @Qualifier("snapshotConsumerFactory") ConsumerFactory<String, SensorsSnapshotAvro> consumerFactory) {
        super(consumerFactory);
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

    @Override
    protected CommitMode commitMode() {
        return CommitMode.ASYNC_BATCHED;
    }

    @Override
    protected int commitBatchSize() {
        return kafkaProperties.getConsumer().getSnapshot().getCommitBatchSize();
    }
}
