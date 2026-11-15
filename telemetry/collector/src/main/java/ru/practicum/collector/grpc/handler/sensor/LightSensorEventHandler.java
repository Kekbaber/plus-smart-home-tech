package ru.practicum.collector.grpc.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.sensor.LightSensorEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
@RequiredArgsConstructor
public class LightSensorEventHandler extends BaseSensorEventHandler {

    private final CollectorService collectorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getLightSensorEvent();

        LightSensorEvent model = new LightSensorEvent();
        model.setLinkQuality(payload.getLinkQuality());
        model.setLuminosity(payload.getLuminosity());

        fillCommonFields(event, model);
        collectorService.collectSensorEvent(model);
    }
}
