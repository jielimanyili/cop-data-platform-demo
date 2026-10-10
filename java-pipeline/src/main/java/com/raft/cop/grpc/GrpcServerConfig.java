package com.raft.cop.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Starts a gRPC server alongside Spring Boot. The REST API (8080) and this gRPC API
 * (default 6565) expose the same operational picture over two transports — REST for
 * browsers/tooling, gRPC for typed, low-latency internal service calls.
 */
@Configuration
public class GrpcServerConfig {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerConfig.class);

    @Bean(destroyMethod = "shutdown")
    public Server grpcServer(
            OperationalPictureGrpcService service,
            @Value("${app.grpc.port:6565}") int port) throws IOException {
        Server server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
        log.info("gRPC server started on port {}", server.getPort());
        return server;
    }
}
