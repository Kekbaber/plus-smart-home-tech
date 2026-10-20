package ru.practicum.collector.model.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.collector.model.hub.enums.ConditionOperator;
import ru.practicum.collector.model.hub.enums.ConditionType;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperator operation;
    private Object value;
}
