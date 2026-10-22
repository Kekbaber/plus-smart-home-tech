package ru.practicum.collector.grpc.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.sensor.MotionSensorEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
@RequiredArgsConstructor
public class MotionSensorEventHandler extends BaseSensorEventHandler {

    private final CollectorService collectorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getMotionSensorEvent();

        MotionSensorEvent model = new MotionSensorEvent();
        model.setLinkQuality(payload.getLinkQuality());
        model.setMotion(payload.getMotion());
        model.setVoltage(payload.getVoltage());

        fillCommonFields(event, model);
        collectorService.collectSensorEvent(model);
    }
}