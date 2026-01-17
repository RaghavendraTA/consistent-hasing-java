package com.raghavendrata.consistenthashing.services;

import com.google.common.hash.Hashing;
import com.raghavendrata.consistenthashing.config.Node;
import com.raghavendrata.consistenthashing.interfaces.HashRing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class ConsistentHashRingService implements HashRing {

    private final SortedMap<Long, Node> ring = new TreeMap<>();
    private final int virtualNodes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRingService(
            @Value("${hashing.virtual-nodes:100}") int virtualNodes
    ) {
        this.virtualNodes = virtualNodes;
    }

    @Override
    public void addNode(Node node) {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < virtualNodes * node.weight(); i++) {
                long hash = hash(node.id() + "#" + i);
                ring.put(hash, node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeNode(String nodeId) {
        lock.writeLock().lock();
        try {
            ring.entrySet().removeIf(e -> e.getValue().id().equals(nodeId));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Node getNode(String key) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) return null;
            long hash = hash(key);
            SortedMap<Long, Node> tail = ring.tailMap(hash);
            return tail.isEmpty()
                    ? ring.get(ring.firstKey())
                    : tail.get(tail.firstKey());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Node> snapshot() {
        return ring;
    }

    private long hash(String key) {
        return Hashing.murmur3_128()
                .hashUnencodedChars(key)
                .asLong();
    }
}
