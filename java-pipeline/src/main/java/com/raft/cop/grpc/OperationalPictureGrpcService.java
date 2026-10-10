package com.raft.cop.grpc;

import com.raft.cop.model.AssetAggregate;
import com.raft.cop.service.PictureQueryService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

/**
 * gRPC implementation of the {@code OperationalPictureService}: serves the operational
 * picture over a typed protobuf contract (same data as the REST {@code /api/picture}).
 */
@Component
public class OperationalPictureGrpcService
        extends OperationalPictureServiceGrpc.OperationalPictureServiceImplBase {

    private final PictureQueryService query;

    public OperationalPictureGrpcService(PictureQueryService query) {
        this.query = query;
    }

    @Override
    public void getAsset(GetAssetRequest request, StreamObserver<AssetSnapshot> responseObserver) {
        query.asset(request.getAssetId())
                .map(AssetAggregateConverter::toProto)
                .ifPresentOrElse(agg -> {
                    responseObserver.onNext(agg);
                    responseObserver.onCompleted();
                }, () -> responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("asset not found: " + request.getAssetId())
                                .asRuntimeException()));
    }

    @Override
    public void getPicture(GetPictureRequest request, StreamObserver<AssetSnapshot> responseObserver) {
        for (AssetAggregate agg : query.picture()) {
            responseObserver.onNext(AssetAggregateConverter.toProto(agg));
        }
        responseObserver.onCompleted();
    }
}
