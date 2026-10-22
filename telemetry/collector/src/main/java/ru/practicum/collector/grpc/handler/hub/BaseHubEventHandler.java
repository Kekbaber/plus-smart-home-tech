package ru.practicum.collector.grpc.handler.hub;

import ru.practicum.collector.grpc.handler.HubEventHandler;
import ru.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

import java.time.Instant;

public abstract class BaseHubEventHandler implements HubEventHandler {

    protected void fillCommonFields(HubEventProto proto, HubEvent event) {
        event.setHubId(proto.getHubId());
        if (proto.hasTimestamp()) {
            event.setTimestamp(Instant.ofEpochSecond(
                    proto.getTimestamp().getSeconds(),
                    proto.getTimestamp().getNanos()));
        }
    }
}