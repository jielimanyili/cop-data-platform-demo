package com.raft.cop.grpc;

import com.raft.cop.model.AssetAggregate;
import com.raft.cop.service.PictureQueryService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the gRPC service end-to-end over the in-process transport (no sockets/netty). */
class OperationalPictureGrpcServiceTest {

    private Server server;
    private ManagedChannel channel;
    private OperationalPictureServiceGrpc.OperationalPictureServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws IOException {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new OperationalPictureGrpcService(fakeQuery()))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = OperationalPictureServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void getAssetReturnsSnapshot() {
        AssetSnapshot snapshot = stub.getAsset(GetAssetRequest.newBuilder().setAssetId("V-1024").build());

        assertThat(snapshot.getAssetId()).isEqualTo("V-1024");
        assertThat(snapshot.getCallsign()).isEqualTo("Ghost 7");
        assertThat(snapshot.getUnit()).isEqualTo("3rd Cavalry");
        assertThat(snapshot.getEventCount()).isEqualTo(5L);
        assertThat(snapshot.getAvgSpeed()).isEqualTo(60.0);
    }

    @Test
    void getAssetReturnsNotFoundForUnknownAsset() {
        assertThatThrownBy(() -> stub.getAsset(GetAssetRequest.newBuilder().setAssetId("NOPE").build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(Status.fromThrowable(e).getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void getPictureStreamsEveryAsset() {
        Iterator<AssetSnapshot> snapshots = stub.getPicture(GetPictureRequest.getDefaultInstance());
        List<AssetSnapshot> all = new ArrayList<>();
        snapshots.forEachRemaining(all::add);

        assertThat(all).hasSize(1);
        assertThat(all.get(0).getAssetId()).isEqualTo("V-1024");
    }

    private static PictureQueryService fakeQuery() {
        AssetAggregate aggregate = new AssetAggregate(
                "V-1024", 1_000L, 1_010L, 5L, 300.0, 80.0, 40.0, 35.0, "3rd Cavalry", "Ghost 7");
        return new PictureQueryService() {
            @Override
            public List<AssetAggregate> picture() {
                return List.of(aggregate);
            }

            @Override
            public Optional<AssetAggregate> asset(String assetId) {
                return "V-1024".equals(assetId) ? Optional.of(aggregate) : Optional.empty();
            }
        };
    }
}
