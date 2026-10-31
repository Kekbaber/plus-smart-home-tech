package ru.practicum.analyzer.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.analyzer.model.Action;
import ru.practicum.analyzer.model.Condition;
import ru.practicum.analyzer.model.enums.ActionType;
import ru.practicum.analyzer.model.enums.ConditionOperation;
import ru.practicum.analyzer.model.enums.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HubEventMapper {

    public static Condition toCondition(ScenarioConditionAvro avro) {
        Condition condition = new Condition();
        condition.setType(ConditionType.valueOf(avro.getType().name()));
        condition.setOperation(ConditionOperation.valueOf(avro.getOperation().name()));
        condition.setValue(toIntValue(avro.getValue()));
        return condition;
    }

    public static Action toAction(DeviceActionAvro avro) {
        Action action = new Action();
        action.setType(ActionType.valueOf(avro.getType().name()));
        action.setValue(avro.getValue());
        return action;
    }

    private static Integer toIntValue(Object value) {
        return switch (value) {
            case null -> null;
            case Boolean b -> b ? 1 : 0;
            case Integer i -> i;
            default -> throw new IllegalArgumentException(
                    "Unsupported condition value type: " + value.getClass().getName());
        };
    }
}