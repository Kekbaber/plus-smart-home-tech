package ru.practicum.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.practicum.analyzer.model.Action;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DeviceActionSender {

    private static final long DEADLINE_SECONDS = 5;

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public DeviceActionSender(
            @GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void send(String hubId, String scenarioName, String sensorId, Action action, Instant timestamp) {
        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(toDeviceActionProto(sensorId, action))
                .setTimestamp(toTimestamp(timestamp))
                .build();

        try {
            hubRouterClient.withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS).handleDeviceAction(request);

            log.info("Action {} for sensor {} (scenario '{}', hub {}) sent to hub-router",
                    action.getType(), sensorId, scenarioName, hubId);
        } catch (RuntimeException e) {
            log.error("Failed to send action {} for sensor {} (scenario '{}', hub {})",
                    action.getType(), sensorId, scenarioName, hubId, e);
        }
    }

    private DeviceActionProto toDeviceActionProto(String sensorId, Action action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    private Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}