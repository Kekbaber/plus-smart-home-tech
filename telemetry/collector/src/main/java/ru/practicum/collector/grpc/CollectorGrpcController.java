package ru.practicum.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.practicum.collector.grpc.handler.HubEventHandler;
import ru.practicum.collector.grpc.handler.SensorEventHandler;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@GrpcService
@Slf4j
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorHandlers;
    private final Map<HubEventProto.PayloadCase, HubEventHandler> hubHandlers;

    public CollectorGrpcController(Set<SensorEventHandler> sensorHandlers,
                                   Set<HubEventHandler> hubHandlers) {
        this.sensorHandlers = sensorHandlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        SensorEventHandler::getMessageType,
                        Function.identity()));
        this.hubHandlers = hubHandlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        HubEventHandler::getMessageType,
                        Function.identity()));
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Collecting sensor event: {}", request);

            SensorEventHandler handler = sensorHandlers.get(request.getPayloadCase());
            if (handler == null) {
                throw new IllegalArgumentException(
                        "No handler for sensor event: " + request.getPayloadCase());
            }

            handler.handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error collecting sensor event", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Collecting hub event: {}", request);

            HubEventHandler handler = hubHandlers.get(request.getPayloadCase());
            if (handler == null) {
                throw new IllegalArgumentException(
                        "No handler for hub event: " + request.getPayloadCase());
            }

            handler.handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error collecting hub event", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}