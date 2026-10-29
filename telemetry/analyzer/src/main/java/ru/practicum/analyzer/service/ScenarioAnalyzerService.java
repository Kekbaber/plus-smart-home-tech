package ru.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.practicum.analyzer.model.Condition;
import ru.practicum.analyzer.model.Scenario;
import ru.practicum.analyzer.model.ScenarioCondition;
import ru.practicum.analyzer.model.enums.ConditionOperation;
import ru.practicum.analyzer.model.enums.ConditionType;
import ru.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioAnalyzerService {

    private static final Map<ConditionType, Function<Object, Integer>> VALUE_EXTRACTORS = Map.of(
            ConditionType.MOTION, data -> data instanceof MotionSensorAvro m ? boolToInt(m.getMotion()) : null,
            ConditionType.LUMINOSITY, data -> data instanceof LightSensorAvro l ? l.getLuminosity() : null,
            ConditionType.SWITCH,
            data -> data instanceof SwitchSensorAvro s ? boolToInt(s.getState()) : null,
            ConditionType.TEMPERATURE, data -> switch (data) {
                case ClimateSensorAvro climate -> climate.getTemperatureC();
                case TemperatureSensorAvro temperature -> temperature.getTemperatureC();
                default -> null;
            },
            ConditionType.CO2LEVEL, data -> data instanceof ClimateSensorAvro c ? c.getCo2Level() : null,
            ConditionType.HUMIDITY, data -> data instanceof ClimateSensorAvro c ? c.getHumidity() : null
    );

    private static final Map<ConditionOperation, BiPredicate<Integer, Integer>> OPERATIONS = Map.of(
            ConditionOperation.EQUALS, Integer::equals,
            ConditionOperation.GREATER_THAN, (actual, expected) -> actual > expected,
            ConditionOperation.LOWER_THAN, (actual, expected) -> actual < expected
    );

    private final ScenarioRepository scenarioRepository;
    private final DeviceActionSender deviceActionSender;

    public void analyze(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("No scenarios configured for hub {}", hubId);
            return;
        }

        Map<String, SensorStateAvro> states = snapshot.getSensorsState();

        List<Scenario> triggered = scenarios.stream()
                .filter(scenario -> isTriggered(scenario, states))
                .toList();

        triggered.forEach(scenario -> {
            log.info("Scenario '{}' triggered for hub {}", scenario.getName(), hubId);
            execute(scenario, snapshot);
        });

        log.debug("Analyzed {} scenarios for hub {}, triggered: {}",
                scenarios.size(), hubId, triggered.size());
    }

    private boolean isTriggered(Scenario scenario, Map<String, SensorStateAvro> states) {
        return !scenario.getConditions().isEmpty()
                && scenario.getConditions().stream()
                .allMatch(link -> conditionMatches(link, states));
    }

    private boolean conditionMatches(ScenarioCondition link, Map<String, SensorStateAvro> states) {
        Condition condition = link.getCondition();
        if (condition.getValue() == null) {
            return false;
        }

        SensorStateAvro state = states.get(link.getSensor().getId());
        if (state == null) {
            return false;
        }

        Function<Object, Integer> extractor = VALUE_EXTRACTORS.get(condition.getType());
        BiPredicate<Integer, Integer> operation = OPERATIONS.get(condition.getOperation());
        if (extractor == null || operation == null) {
            log.warn("Unsupported condition type/operation: {}/{}",
                    condition.getType(), condition.getOperation());
            return false;
        }

        Integer actual = extractor.apply(state.getData());
        return actual != null && operation.test(actual, condition.getValue());
    }

    private void execute(Scenario scenario, SensorsSnapshotAvro snapshot) {
        scenario.getActions().forEach(link ->
                deviceActionSender.send(
                        snapshot.getHubId(),
                        scenario.getName(),
                        link.getSensor().getId(),
                        link.getAction(),
                        snapshot.getTimestamp()));
    }

    private static int boolToInt(boolean value) {
        return value ? 1 : 0;
    }
}
