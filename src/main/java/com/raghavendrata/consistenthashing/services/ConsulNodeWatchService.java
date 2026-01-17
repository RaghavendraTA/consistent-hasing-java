package com.raghavendrata.consistenthashing.services;

import com.ecwid.consul.v1.ConsulClient;
import com.raghavendrata.consistenthashing.config.Node;
import com.raghavendrata.consistenthashing.interfaces.HashRing;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
public class ConsulNodeWatchService {

    private final ConsulClient consulClient;
    private final HashRing ring;

    public ConsulNodeWatchService(ConsulClient consulClient, HashRing ring) {
        this.consulClient = consulClient;
        this.ring = ring;
    }

    @Scheduled(fixedDelay = 5000)
    public void syncNodes() {
        var services = consulClient.getHealthServices(
                "worker-service",
                true,
                null
        ).getValue();

        Set<String> active = new HashSet<>();

        for (var service : services) {
            String nodeId = service.getService().getId();
            active.add(nodeId);

            ring.addNode(new Node(
                    nodeId,
                    service.getService().getAddress(),
                    service.getService().getPort(),
                    1
            ));
        }

        // remove stale nodes
        ring.snapshot().values().stream()
                .map(Node::id)
                .filter(id -> !active.contains(id))
                .forEach(ring::removeNode);
    }
}
