package ru.practicum.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.practicum.collector.grpc.mapper.HubEventProtoMapper;
import ru.practicum.collector.grpc.mapper.SensorEventProtoMapper;
import ru.practicum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@GrpcService
@RequiredArgsConstructor
@Slf4j
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final CollectorService collectorService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Collecting sensor event: {}", request);
            collectorService.collectSensorEvent(SensorEventProtoMapper.toModel(request));
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
            collectorService.collectHubEvent(HubEventProtoMapper.toModel(request));
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
