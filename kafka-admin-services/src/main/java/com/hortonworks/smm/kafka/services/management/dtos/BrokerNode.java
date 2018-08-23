/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.smm.kafka.services.management.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents Kafka broker node with respective details like id, host, port and rack.
 * {@link Node#noNode()} is represented as {@link #NO_NODE}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerNode {

    public static final BrokerNode NO_NODE = new BrokerNode(Node.noNode().id(),
                                                            Node.noNode().host(),
                                                            Node.noNode().port(),
                                                            Node.noNode().rack());

    private static volatile Map<Node, BrokerNode> brokerNodePool = new HashMap<>();

    @JsonProperty
    private int id;

    @JsonProperty
    private String host;

    @JsonProperty
    private int port;

    @JsonProperty
    private String rack;

    private BrokerNode() {
    }

    private BrokerNode(int id, String host, int port, String rack) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.rack = rack;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String rack() {
        return rack;
    }

    /**
     * Returns respective BrokerNode for the given node. If the given node is null or {@link Node#noNode()} then it
     * returns {@link #NO_NODE}. It will never return null.
     *
     * @param node node instance for which BrokerNode to be created.
     * @return Returns respective BrokerNode for the given node.
     */
    public static BrokerNode from(Node node) {
        return node == null || Node.noNode().equals(node)
               ? NO_NODE
               : brokerNodePool.getOrDefault(node, NO_NODE);
    }

    public static void refreshPool(Collection<Node> nodes) {
        Map<Node, BrokerNode> nodeMap = new HashMap<>();
        for (Node node : nodes) {
            nodeMap.put(node, new BrokerNode(node.id(), node.host(), node.port(), node.rack()));
        }
        brokerNodePool = nodeMap;
    }

    public Node toNode() {
        return new Node(id, host, port, rack);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerNode that = (BrokerNode) o;
        return id == that.id &&
               port == that.port &&
               Objects.equals(host, that.host) &&
               Objects.equals(rack, that.rack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port, rack);
    }

    @Override
    public String toString() {
        return "BrokerNode{" +
               "id=" + id +
               ", host='" + host + '\'' +
               ", port=" + port +
               ", rack='" + rack + '\'' +
               '}';
    }
}
