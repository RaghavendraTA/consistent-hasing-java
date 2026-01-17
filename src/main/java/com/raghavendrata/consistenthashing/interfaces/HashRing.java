package com.raghavendrata.consistenthashing.interfaces;

import com.raghavendrata.consistenthashing.config.Node;

import java.util.Map;

public interface HashRing {
    void addNode(Node node);
    void removeNode(String nodeId);
    Node getNode(String key);
    Map<Long, Node> snapshot();
}
