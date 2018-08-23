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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartitionInfo {

    @JsonProperty
    private int partition;

    @JsonProperty
    private BrokerNode leader;

    @JsonProperty
    private List<BrokerNode> replicas;

    @JsonProperty
    private List<BrokerNode> isr;

    private TopicPartitionInfo() {
    }

    private TopicPartitionInfo(int partition,
                               BrokerNode leader,
                               List<BrokerNode> replicas,
                               List<BrokerNode> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
    }

    public static TopicPartitionInfo from(org.apache.kafka.common.TopicPartitionInfo topicPartitionInfo) {
        return new TopicPartitionInfo(topicPartitionInfo.partition(),
                                      BrokerNode.from(topicPartitionInfo.leader()),
                                      topicPartitionInfo.replicas()
                                                        .stream()
                                                        .map(BrokerNode::from)
                                                        .collect(Collectors.toList()),
                                      topicPartitionInfo.isr()
                                                        .stream()
                                                        .map(BrokerNode::from)
                                                        .collect(Collectors.toList()));
    }

    public int partition() {
        return partition;
    }

    public BrokerNode leader() {
        return leader;
    }

    public boolean leaderExists() {
        return leader.id() != -1;
    }

    public List<BrokerNode> replicas() {
        return replicas;
    }

    public List<BrokerNode> isr() {
        return isr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartitionInfo that = (TopicPartitionInfo) o;
        return partition == that.partition &&
                Objects.equals(leader, that.leader) &&
                Objects.equals(replicas, that.replicas) &&
                Objects.equals(isr, that.isr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, leader, replicas, isr);
    }

    @Override
    public String toString() {
        return "TopicPartitionInfo{" +
                "partition=" + partition +
                ", leader=" + leader +
                ", replicas=" + replicas +
                ", isr=" + isr +
                '}';
    }
}
