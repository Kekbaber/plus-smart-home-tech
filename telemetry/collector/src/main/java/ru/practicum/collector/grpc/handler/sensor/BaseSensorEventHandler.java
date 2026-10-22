package ru.practicum.collector.grpc.handler.sensor;

import ru.practicum.collector.grpc.handler.SensorEventHandler;
import ru.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;

public abstract class BaseSensorEventHandler implements SensorEventHandler {

    protected void fillCommonFields(SensorEventProto proto, SensorEvent event) {
        event.setId(proto.getId());
        event.setHubId(proto.getHubId());
        if (proto.hasTimestamp()) {
            event.setTimestamp(Instant.ofEpochSecond(
                    proto.getTimestamp().getSeconds(),
                    proto.getTimestamp().getNanos()));
        }
    }
}