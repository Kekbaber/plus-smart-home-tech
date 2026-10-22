package ru.practicum.collector.grpc.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.hub.DeviceAddedEvent;
import ru.practicum.collector.model.hub.enums.DeviceType;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
@RequiredArgsConstructor
public class DeviceAddedHubEventHandler extends BaseHubEventHandler {

    private final CollectorService collectorService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getDeviceAdded();

        DeviceAddedEvent model = new DeviceAddedEvent();
        model.setId(payload.getId());
        model.setDeviceType(DeviceType.valueOf(payload.getType().name()));

        fillCommonFields(event, model);
        collectorService.collectHubEvent(model);
    }
}
