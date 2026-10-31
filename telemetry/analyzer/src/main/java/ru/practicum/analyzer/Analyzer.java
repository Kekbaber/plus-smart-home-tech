package ru.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.practicum.analyzer.service.HubEventProcessor;
import ru.practicum.analyzer.service.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    private static final long SHUTDOWN_JOIN_TIMEOUT_MILLIS = 5000;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);
        // Spring сам закроет контекст (gRPC-канал, datasource) при завершении JVM
        context.registerShutdownHook();

        final HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        final SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        // при завершении JVM будим оба цикла опроса и ждём их корректной остановки
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hubEventProcessor.wakeup();
            snapshotProcessor.wakeup();
            try {
                hubEventsThread.join(SHUTDOWN_JOIN_TIMEOUT_MILLIS);
                mainThread.join(SHUTDOWN_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "analyzer-shutdown"));

        snapshotProcessor.run();
    }
}
