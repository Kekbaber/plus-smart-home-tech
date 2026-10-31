package ru.practicum.collector.grpc.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.sensor.ClimateSensorEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
@RequiredArgsConstructor
public class ClimateSensorEventHandler extends BaseSensorEventHandler {

    private final CollectorService collectorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getClimateSensorEvent();

        ClimateSensorEvent model = new ClimateSensorEvent();
        model.setTemperatureC(payload.getTemperatureC());
        model.setHumidity(payload.getHumidity());
        model.setCo2Level(payload.getCo2Level());

        fillCommonFields(event, model);
        collectorService.collectSensorEvent(model);
    }
}
