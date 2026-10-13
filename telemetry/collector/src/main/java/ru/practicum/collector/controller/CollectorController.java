package ru.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.collector.model.hub.HubEvent;
import ru.practicum.collector.model.sensor.SensorEvent;
import ru.practicum.collector.service.CollectorService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
@Slf4j
public class CollectorController {
    private final CollectorService collectorService;

    @PostMapping("/sensors")
    public void collectSensorsEvent(@Valid @RequestBody SensorEvent sensorEvent) {
        log.info("CollectorController collectSensorsEvent sensorEvent={}", sensorEvent);
        collectorService.collectSensorEvent(sensorEvent);
    }

    @PostMapping("/hubs")
    public void collectHubsEvent(@Valid @RequestBody HubEvent hubEvent) {
        log.info("CollectorController collectHubsEvent hubEvent={}", hubEvent);
        collectorService.collectHubEvent(hubEvent);
    }
}
