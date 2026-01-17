package com.raghavendrata.consistenthashing.grpc;

import com.raghavendrata.consistenthashing.config.Node;
import com.raghavendrata.consistenthashing.interfaces.HashRing;
import hashing.ConsistentHashing;
import hashing.HashingServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class HashingServerService extends HashingServiceGrpc.HashingServiceImplBase {

    private final HashRing ring;

    public HashingServerService(HashRing ring) {
        this.ring = ring;
    }

    @Override
    public void registerNode(ConsistentHashing.NodeInfo request, StreamObserver<ConsistentHashing.Ack> responseObserver) {
        ring.addNode(new Node(
            request.getNodeId(),
            request.getHost(),
            request.getPort(),
            request.getWeight()
        ));
        responseObserver.onNext(getSuccessTrue());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterNode(ConsistentHashing.NodeInfo request, StreamObserver<ConsistentHashing.Ack> responseObserver) {
        ring.removeNode(request.getNodeId());
        responseObserver.onNext(getSuccessTrue());
        responseObserver.onCompleted();
    }

    @Override
    public void resolveKey(
            ConsistentHashing.KeyRequest request,
            StreamObserver<ConsistentHashing.NodeInfo> responseObserver) {

        Node node = ring.getNode(request.getKey());

        if (node == null) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription("No nodes available")
                            .asRuntimeException()
            );
            return;
        }

        responseObserver.onNext(node.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void getRing(ConsistentHashing.Empty request, StreamObserver<ConsistentHashing.RingState> responseObserver) {
        ConsistentHashing.RingState state = ConsistentHashing.RingState.newBuilder()
                .addAllNodes(ring.snapshot()
                    .values()
                    .stream()
                    .map(x -> ConsistentHashing.NodeInfo.newBuilder()
                            .setNodeId(x.id())
                            .setHost(x.host())
                            .setPort(x.port())
                            .setWeight(x.weight())
                            .build()
                    )
                    .toList()
                )
                .build();
        responseObserver.onNext(state);
    }

    private ConsistentHashing.Ack getSuccessTrue() {
        return ConsistentHashing.Ack.newBuilder().setSuccess(true).build();
    }
}
