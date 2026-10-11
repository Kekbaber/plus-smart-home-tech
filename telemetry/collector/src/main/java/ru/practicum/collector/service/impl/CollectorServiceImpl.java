package ru.practicum.collector.service.impl;

import org.springframework.stereotype.Service;
import ru.practicum.collector.model.hub.HubEvent;
import ru.practicum.collector.model.sensor.SensorEvent;
import ru.practicum.collector.service.CollectorService;

@Service
public class CollectorServiceImpl implements CollectorService {

    @Override
    public void collectSensorEvent(SensorEvent sensorEvent) {

    }

    @Override
    public void collectHubEvent(HubEvent hubEvent) {

    }
}
