package ru.practicum.collector.grpc.mapper;

import com.google.protobuf.Timestamp;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.collector.model.sensor.*;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;

import java.time.Instant;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SensorEventProtoMapper {

    public static SensorEvent toModel(SensorEventProto proto) {
        SensorEvent event = switch (proto.getPayloadCase()) {
            case MOTION_SENSOR -> toMotionSensor(proto.getMotionSensor());
            case TEMPERATURE_SENSOR -> toTemperatureSensor(proto.getTemperatureSensor());
            case LIGHT_SENSOR -> toLightSensor(proto.getLightSensor());
            case CLIMATE_SENSOR -> toClimateSensor(proto.getClimateSensor());
            case SWITCH_SENSOR -> toSwitchSensor(proto.getSwitchSensor());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Unknown event: " + proto.getPayloadCase());
        };

        event.setHubId(proto.getHubId());
        event.setId(proto.getId());
        if (proto.hasTimestamp()) {
            event.setTimestamp(toInstant(proto.getTimestamp()));
        }

        return event;
    }

    private static MotionSensorEvent toMotionSensor(MotionSensorProto e) {
        MotionSensorEvent event = new MotionSensorEvent();
        event.setLinkQuality(e.getLinkQuality());
        event.setMotion(e.getMotion());
        event.setVoltage(e.getVoltage());
        return event;
    }

    private static TemperatureSensorEvent toTemperatureSensor(TemperatureSensorProto e) {
        TemperatureSensorEvent event = new TemperatureSensorEvent();
        event.setTemperatureC(e.getTemperatureC());
        event.setTemperatureF(e.getTemperatureF());
        return event;
    }

    private static LightSensorEvent toLightSensor(LightSensorProto e) {
        LightSensorEvent event = new LightSensorEvent();
        event.setLinkQuality(e.getLinkQuality());
        event.setLuminosity(e.getLuminosity());
        return event;
    }

    private static ClimateSensorEvent toClimateSensor(ClimateSensorProto e) {
        ClimateSensorEvent event = new ClimateSensorEvent();
        event.setTemperatureC(e.getTemperatureC());
        event.setHumidity(e.getHumidity());
        event.setCo2Level(e.getCo2Level());
        return event;
    }

    private static SwitchSensorEvent toSwitchSensor(SwitchSensorProto e) {
        SwitchSensorEvent event = new SwitchSensorEvent();
        event.setState(e.getState());
        return event;
    }

    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}