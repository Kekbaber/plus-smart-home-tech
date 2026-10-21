package ru.practicum.collector.grpc.mapper;

import com.google.protobuf.Timestamp;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.collector.model.hub.*;
import ru.practicum.collector.model.hub.enums.ActionType;
import ru.practicum.collector.model.hub.enums.ConditionOperator;
import ru.practicum.collector.model.hub.enums.ConditionType;
import ru.practicum.collector.model.hub.enums.DeviceType;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;

import java.time.Instant;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HubEventProtoMapper {

    public static HubEvent toModel(HubEventProto proto) {
        HubEvent event = switch (proto.getPayloadCase()) {
            case DEVICE_ADDED -> toDeviceAdded(proto.getDeviceAdded());
            case DEVICE_REMOVED -> toDeviceRemoved(proto.getDeviceRemoved());
            case SCENARIO_ADDED -> toScenarioAdded(proto.getScenarioAdded());
            case SCENARIO_REMOVED -> toScenarioRemoved(proto.getScenarioRemoved());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Unknown event: " + proto.getPayloadCase());
        };

        event.setHubId(proto.getHubId());
        if (proto.hasTimestamp()) {
            event.setTimestamp(toInstant(proto.getTimestamp()));
        }

        return event;
    }

    private static DeviceAddedEvent toDeviceAdded(DeviceAddedEventProto e) {
        DeviceAddedEvent event = new DeviceAddedEvent();
        event.setId(e.getId());
        event.setDeviceType(DeviceType.valueOf(e.getType().name()));
        return event;
    }

    private static DeviceRemovedEvent toDeviceRemoved(DeviceRemovedEventProto e) {
        DeviceRemovedEvent event = new DeviceRemovedEvent();
        event.setId(e.getId());
        return event;
    }

    private static ScenarioAddedEvent toScenarioAdded(ScenarioAddedEventProto e) {
        List<ScenarioCondition> conditions = e.getConditionList().stream()
                .map(HubEventProtoMapper::toScenarioCondition)
                .toList();

        List<DeviceAction> actions = e.getActionList().stream()
                .map(HubEventProtoMapper::toDeviceAction)
                .toList();

        ScenarioAddedEvent event = new ScenarioAddedEvent();
        event.setName(e.getName());
        event.setConditions(conditions);
        event.setActions(actions);
        return event;
    }

    private static ScenarioRemovedEvent toScenarioRemoved(ScenarioRemovedEventProto e) {
        ScenarioRemovedEvent event = new ScenarioRemovedEvent();
        event.setName(e.getName());
        return event;
    }

    private static ScenarioCondition toScenarioCondition(ScenarioConditionProto e) {
        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId(e.getSensorId());
        condition.setType(ConditionType.valueOf(e.getType().name()));
        condition.setOperation(ConditionOperator.valueOf(e.getOperation().name()));

        if (e.getValueCase() == ScenarioConditionProto.ValueCase.BOOL_VALUE) {
            condition.setValue(e.getBoolValue());
        } else if (e.getValueCase() == ScenarioConditionProto.ValueCase.INT_VALUE) {
            condition.setValue(e.getIntValue());
        }

        return condition;
    }

    private static DeviceAction toDeviceAction(DeviceActionProto e) {
        DeviceAction action = new DeviceAction();
        action.setSensorId(e.getSensorId());
        action.setType(ActionType.valueOf(e.getType().name()));

        if (e.hasValue()) {
            action.setValue(e.getValue());
        }

        return action;
    }

    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}