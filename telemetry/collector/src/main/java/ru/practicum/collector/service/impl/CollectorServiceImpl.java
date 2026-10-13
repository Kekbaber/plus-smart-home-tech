package ru.practicum.collector.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.practicum.collector.kafka.KafkaEventProducer;
import ru.practicum.collector.mapper.HubEventMapper;
import ru.practicum.collector.mapper.SensorEventMapper;
import ru.practicum.collector.model.hub.HubEvent;
import ru.practicum.collector.model.sensor.SensorEvent;
import ru.practicum.collector.service.CollectorService;

@Service
@RequiredArgsConstructor
@Slf4j
public class CollectorServiceImpl implements CollectorService {

    private final KafkaEventProducer kafkaProducer;

    @Override
    public void collectSensorEvent(SensorEvent sensorEvent) {
        var avro = SensorEventMapper.toAvro(sensorEvent);
        kafkaProducer.sendSensorEvent(avro);
        log.info("Sensor event sent to kafka");
    }

    @Override
    public void collectHubEvent(HubEvent hubEvent) {
        var avro = HubEventMapper.toAvro(hubEvent);
        kafkaProducer.sendHubEvent(avro);
        log.info("Hub event sent to kafka");
    }

}
