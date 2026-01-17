package com.raghavendrata.consistenthashing.config;
import hashing.ConsistentHashing;

import java.util.Objects;

public final class Node {

    private final String id;
    private final String host;
    private final int port;
    private final int weight;

    public Node(String id, String host, int port, int weight) {
        this.id = Objects.requireNonNull(id, "id");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.weight = weight <= 0 ? 1 : weight;
    }

    public String id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public int weight() {
        return weight;
    }

    /* -----------------------------
       Equality based ONLY on nodeId
       ----------------------------- */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return id.equals(node.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /* -----------------------------
       gRPC mapping helper
       ----------------------------- */
    public ConsistentHashing.NodeInfo toProto() {
        return ConsistentHashing.NodeInfo.newBuilder()
                .setNodeId(id)
                .setHost(host)
                .setPort(port)
                .setWeight(weight)
                .build();
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", weight=" + weight +
                '}';
    }
}
