package ru.practicum.collector.grpc.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.hub.ScenarioRemovedEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
@RequiredArgsConstructor
public class ScenarioRemovedHubEventHandler extends BaseHubEventHandler {

    private final CollectorService collectorService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getScenarioRemoved();

        ScenarioRemovedEvent model = new ScenarioRemovedEvent();
        model.setName(payload.getName());

        fillCommonFields(event, model);
        collectorService.collectHubEvent(model);
    }
}
