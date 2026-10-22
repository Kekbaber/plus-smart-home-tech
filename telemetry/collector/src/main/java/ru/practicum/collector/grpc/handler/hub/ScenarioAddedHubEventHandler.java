package ru.practicum.collector.grpc.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.hub.DeviceAction;
import ru.practicum.collector.model.hub.ScenarioAddedEvent;
import ru.practicum.collector.model.hub.ScenarioCondition;
import ru.practicum.collector.model.hub.enums.ActionType;
import ru.practicum.collector.model.hub.enums.ConditionOperator;
import ru.practicum.collector.model.hub.enums.ConditionType;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;

@Component
@RequiredArgsConstructor
public class ScenarioAddedHubEventHandler extends BaseHubEventHandler {

    private final CollectorService collectorService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getScenarioAdded();

        ScenarioAddedEvent model = new ScenarioAddedEvent();
        model.setName(payload.getName());
        model.setConditions(payload.getConditionList().stream()
                .map(this::toCondition)
                .toList());
        model.setActions(payload.getActionList().stream()
                .map(this::toAction)
                .toList());

        fillCommonFields(event, model);
        collectorService.collectHubEvent(model);
    }

    private ScenarioCondition toCondition(ScenarioConditionProto proto) {
        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId(proto.getSensorId());
        condition.setType(ConditionType.valueOf(proto.getType().name()));
        condition.setOperation(ConditionOperator.valueOf(proto.getOperation().name()));

        if (proto.getValueCase() == ScenarioConditionProto.ValueCase.BOOL_VALUE) {
            condition.setValue(proto.getBoolValue());
        } else if (proto.getValueCase() == ScenarioConditionProto.ValueCase.INT_VALUE) {
            condition.setValue(proto.getIntValue());
        }

        return condition;
    }

    private DeviceAction toAction(DeviceActionProto proto) {
        DeviceAction action = new DeviceAction();
        action.setSensorId(proto.getSensorId());
        action.setType(ActionType.valueOf(proto.getType().name()));

        if (proto.hasValue()) {
            action.setValue(proto.getValue());
        }

        return action;
    }
}
