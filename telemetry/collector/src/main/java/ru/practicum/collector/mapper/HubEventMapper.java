package ru.practicum.collector.mapper;

import ru.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

public class HubEventMapper {

    public static HubEventAvro toAvro(HubEvent hubEvent) {
        Object payload = switch (hubEvent) {
            case DeviceAddedEvent e -> toDeviceAdded(e);
            case DeviceRemovedEvent e -> toDeviceRemoved(e);
            case ScenarioAddedEvent e -> toScenarioAdded(e);
            case ScenarioRemovedEvent e -> toScenarioRemoved(e);
            default -> null;
        };

        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setPayload(payload)
                .setTimestamp(hubEvent.getTimestamp())
                .build();
    }

    private static DeviceAddedEventAvro toDeviceAdded(DeviceAddedEvent e) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(e.getId())
                .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                .build();
    }

    private static DeviceRemovedEventAvro toDeviceRemoved(DeviceRemovedEvent e) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(e.getId())
                .build();
    }

    private static ScenarioAddedEventAvro toScenarioAdded(ScenarioAddedEvent e) {
        List<ScenarioConditionAvro> conditions = e.getConditions().stream()
                .map(HubEventMapper::toScenarioCondition)
                .toList();

        List<DeviceActionAvro> actions = e.getActions().stream()
                .map(HubEventMapper::toDeviceAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(e.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private static ScenarioRemovedEventAvro toScenarioRemoved(ScenarioRemovedEvent e) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(e.getName())
                .build();
    }

    private static ScenarioConditionAvro toScenarioCondition(ScenarioCondition c) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(c.getSensorId())
                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));

        if (c.getValue() != null) {
            builder.setValue(c.getValue());
        }

        return builder.build();
    }

    private static DeviceActionAvro toDeviceAction(DeviceAction a) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(a.getSensorId())
                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                .setValue(a.getValue());

        return builder.build();
    }
}
