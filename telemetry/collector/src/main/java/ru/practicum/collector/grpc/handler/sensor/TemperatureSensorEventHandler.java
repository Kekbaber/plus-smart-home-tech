package ru.practicum.collector.grpc.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.sensor.TemperatureSensorEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
@RequiredArgsConstructor
public class TemperatureSensorEventHandler extends BaseSensorEventHandler {

    private final CollectorService collectorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getTemperatureSensorEvent();

        TemperatureSensorEvent model = new TemperatureSensorEvent();
        model.setTemperatureC(payload.getTemperatureC());
        model.setTemperatureF(payload.getTemperatureF());

        fillCommonFields(event, model);
        collectorService.collectSensorEvent(model);
    }
}
