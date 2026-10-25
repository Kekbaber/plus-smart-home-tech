package ru.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SnapshotProcessor {
    public void start() {
        log.info("SnapshotProcessor started");
    }
}
