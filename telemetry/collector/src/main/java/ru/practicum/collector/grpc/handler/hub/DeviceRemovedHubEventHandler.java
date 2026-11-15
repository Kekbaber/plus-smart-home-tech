package ru.practicum.collector.grpc.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.hub.DeviceRemovedEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
@RequiredArgsConstructor
public class DeviceRemovedHubEventHandler extends BaseHubEventHandler {

    private final CollectorService collectorService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getDeviceRemoved();

        DeviceRemovedEvent model = new DeviceRemovedEvent();
        model.setId(payload.getId());

        fillCommonFields(event, model);
        collectorService.collectHubEvent(model);
    }
}
