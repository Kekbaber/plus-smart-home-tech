package ru.practicum.collector.grpc.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.collector.model.sensor.SwitchSensorEvent;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
@RequiredArgsConstructor
public class SwitchSensorEventHandler extends BaseSensorEventHandler {

    private final CollectorService collectorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getSwitchSensorEvent();

        SwitchSensorEvent model = new SwitchSensorEvent();
        model.setState(payload.getState());

        fillCommonFields(event, model);
        collectorService.collectSensorEvent(model);
    }
}
