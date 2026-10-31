package ru.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.analyzer.mapper.HubEventMapper;
import ru.practicum.analyzer.model.*;
import ru.practicum.analyzer.repository.ActionRepository;
import ru.practicum.analyzer.repository.ConditionRepository;
import ru.practicum.analyzer.repository.ScenarioRepository;
import ru.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(rollbackFor = Exception.class)
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();

        switch (event.getPayload()) {
            case DeviceAddedEventAvro e -> addDevice(hubId, e);
            case DeviceRemovedEventAvro e -> removeDevice(hubId, e);
            case ScenarioAddedEventAvro e -> addScenario(hubId, e);
            case ScenarioRemovedEventAvro e -> removeScenario(hubId, e);
            case null -> log.warn("Hub event without payload for hub {}", hubId);
            default -> log.warn("Unsupported hub event payload: {}",
                    event.getPayload().getClass().getName());
        }
    }

    private void addDevice(String hubId, DeviceAddedEventAvro event) {
        Sensor existing = sensorRepository.findById(event.getId()).orElse(null);

        if (existing != null) {
            if (hubId.equals(existing.getHubId())) {
                log.debug("Sensor {} already registered in hub {}, skipping duplicate",
                        event.getId(), hubId);
            } else {
                log.warn("Sensor {} is already registered in hub {}, ignoring event for hub {}",
                        event.getId(), existing.getHubId(), hubId);
            }
            return;
        }

        Sensor sensor = new Sensor();
        sensor.setId(event.getId());
        sensor.setHubId(hubId);
        sensorRepository.save(sensor);

        log.info("Sensor {} added to hub {}", event.getId(), hubId);
    }

    private void removeDevice(String hubId, DeviceRemovedEventAvro event) {
        sensorRepository.findByIdAndHubId(event.getId(), hubId)
                .ifPresentOrElse(sensor -> {
                    List<Condition> attachedConditions =
                            conditionRepository.findBySensorId(event.getId());
                    List<Action> attachedActions =
                            actionRepository.findBySensorId(event.getId());

                    sensorRepository.delete(sensor);
                    sensorRepository.flush();

                    deleteOrphans(attachedConditions, attachedActions);
                    log.info("Sensor {} removed from hub {}", event.getId(), hubId);
                }, () -> log.debug("Sensor {} not found in hub {}, skipping",
                        event.getId(), hubId));
    }

    private void addScenario(String hubId, ScenarioAddedEventAvro event) {
        Set<String> sensorIds = collectSensorIds(event);
        Map<String, Sensor> sensors = findHubSensors(sensorIds, hubId);

        if (sensors.size() != sensorIds.size()) {
            throw new IllegalArgumentException(String.format(
                    "Cannot save scenario '%s' for hub %s: sensors %s are missing or belong to another hub",
                    event.getName(), hubId,
                    sensorIds.stream().filter(id -> !sensors.containsKey(id)).toList()));
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> newScenario(hubId, event.getName()));

        scenario.getConditions().clear();
        scenario.getActions().clear();

        event.getConditions().forEach(condition -> scenario.addCondition(
                sensors.get(condition.getSensorId()),
                HubEventMapper.toCondition(condition)));

        event.getActions().forEach(action -> scenario.addAction(
                sensors.get(action.getSensorId()),
                HubEventMapper.toAction(action)));

        scenarioRepository.saveAndFlush(scenario);

        log.info("Scenario '{}' saved for hub {}: {} conditions, {} actions",
                event.getName(), hubId,
                scenario.getConditions().size(), scenario.getActions().size());
    }

    private void removeScenario(String hubId, ScenarioRemovedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresentOrElse(scenario -> {
                    scenarioRepository.delete(scenario);
                    scenarioRepository.flush();

                    log.info("Scenario '{}' removed from hub {}", event.getName(), hubId);
                }, () -> log.debug("Scenario '{}' not found in hub {}, skipping",
                        event.getName(), hubId));
    }

    private Map<String, Sensor> findHubSensors(Set<String> sensorIds, String hubId) {
        return sensorRepository.findAllByIdInAndHubId(sensorIds, hubId).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));
    }

    private Set<String> collectSensorIds(ScenarioAddedEventAvro event) {
        Set<String> sensorIds = new HashSet<>();
        event.getConditions().forEach(condition -> sensorIds.add(condition.getSensorId()));
        event.getActions().forEach(action -> sensorIds.add(action.getSensorId()));
        return sensorIds;
    }

    private Scenario newScenario(String hubId, String name) {
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(name);
        return scenario;
    }

    private void deleteOrphans(List<Condition> conditions, List<Action> actions) {
        if (!conditions.isEmpty()) {
            conditionRepository.deleteAll(conditions);
        }
        if (!actions.isEmpty()) {
            actionRepository.deleteAll(actions);
        }
    }
}