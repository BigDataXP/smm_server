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
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaClusterInfo {

    @JsonProperty
    private Collection<BrokerNode> brokerNodes;

    @JsonProperty
    private BrokerNode controller;

    @JsonProperty
    private String clusterId;

    private KafkaClusterInfo() {
    }

    private KafkaClusterInfo(String clusterId, BrokerNode controller, Collection<BrokerNode> brokerNodes) {
        this.brokerNodes = brokerNodes;
        this.controller = controller;
        this.clusterId = clusterId;
    }

    public static KafkaClusterInfo from(String clusterId, Node controller, Collection<Node> brokerNodes) {
        return new KafkaClusterInfo(clusterId,
                                    BrokerNode.from(controller),
                                    brokerNodes.stream().map(BrokerNode::from).collect(Collectors.toList())
        );
    }


    public Collection<BrokerNode> brokerNodes() {
        return brokerNodes;
    }

    public BrokerNode controller() {
        return controller;
    }

    public String clusterId() {
        return clusterId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaClusterInfo that = (KafkaClusterInfo) o;
        return Objects.equals(brokerNodes, that.brokerNodes) &&
                Objects.equals(controller, that.controller) &&
                Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerNodes, controller, clusterId);
    }

    @Override
    public String toString() {
        return "KafkaClusterInfo{" +
                "brokerNodes=" + brokerNodes +
                ", controller=" + controller +
                ", clusterId='" + clusterId + '\'' +
                '}';
    }
}
