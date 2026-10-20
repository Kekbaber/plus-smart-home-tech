package ru.practicum.collector.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SensorEventMapper {

    public static SensorEventAvro toAvro(SensorEvent sensorEvent) {
        Object payload = switch (sensorEvent) {
            case ClimateSensorEvent e -> toClimateAvro(e);
            case LightSensorEvent e -> toLightAvro(e);
            case TemperatureSensorEvent e -> toTemperatureAvro(e);
            case SwitchSensorEvent e -> toSwitchAvro(e);
            case MotionSensorEvent e -> toMotionAvro(e);
            default -> throw new IllegalArgumentException("Unknown event: " + sensorEvent.getType());
        };

        return SensorEventAvro.newBuilder()
                .setHubId(sensorEvent.getHubId())
                .setId(sensorEvent.getId())
                .setTimestamp(sensorEvent.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private static ClimateSensorAvro toClimateAvro(ClimateSensorEvent e) {
        return ClimateSensorAvro.newBuilder()
                .setCo2Level(e.getCo2Level())
                .setHumidity(e.getHumidity())
                .setTemperatureC(e.getTemperatureC())
                .build();
    }

    private static LightSensorAvro toLightAvro(LightSensorEvent e) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(e.getLinkQuality())
                .setLuminosity(e.getLuminosity())
                .build();
    }

    private static MotionSensorAvro toMotionAvro(MotionSensorEvent e) {
        return MotionSensorAvro.newBuilder()
                .setMotion(e.isMotion())
                .setLinkQuality(e.getLinkQuality())
                .setVoltage(e.getVoltage())
                .build();
    }

    private static SwitchSensorAvro toSwitchAvro(SwitchSensorEvent e) {
        return SwitchSensorAvro.newBuilder()
                .setState(e.isState())
                .build();
    }

    private static TemperatureSensorAvro toTemperatureAvro(TemperatureSensorEvent e) {
        return TemperatureSensorAvro.newBuilder()
                .setHubId(e.getHubId())
                .setId(e.getId())
                .setTemperatureC(e.getTemperatureC())
                .setTemperatureF(e.getTemperatureF())
                .setTimestamp(e.getTimestamp())
                .build();
    }
}
