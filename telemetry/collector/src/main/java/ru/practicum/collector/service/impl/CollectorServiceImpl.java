package ru.practicum.collector.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.practicum.collector.mapper.HubEventMapper;
import ru.practicum.collector.mapper.SensorEventMapper;
import ru.practicum.collector.model.hub.HubEvent;
import ru.practicum.collector.model.sensor.SensorEvent;
import ru.practicum.collector.service.CollectorService;

@Service
@RequiredArgsConstructor
@Slf4j
public class CollectorServiceImpl implements CollectorService {

    private static final String SENSOR_TOPIC = "telemetry.sensors.v1";
    private static final String HUB_TOPIC = "telemetry.hubs.v1";

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Override
    public void collectSensorEvent(SensorEvent sensorEvent) {
        var avro = SensorEventMapper.toAvro(sensorEvent);
        kafkaTemplate.send(SENSOR_TOPIC, sensorEvent.getHubId(), avro);
        log.info("Sensor event {} sent to topic {}", sensorEvent.getId(), SENSOR_TOPIC);
    }

    @Override
    public void collectHubEvent(HubEvent hubEvent) {
        var avro = HubEventMapper.toAvro(hubEvent);
        kafkaTemplate.send(HUB_TOPIC, hubEvent.getHubId(), avro);
        log.info("Hub event {} sent to topic {}", hubEvent.getHubId(), HUB_TOPIC);
    }
}
