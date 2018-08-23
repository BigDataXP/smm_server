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
package com.hortonworks.smm.kafka.services.metric.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.smm.kafka.services.clients.ProducerMetrics;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicSummary;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMinMaxTimestampDeltaValue;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggrTopicMetrics {

    @JsonProperty
    private final String name;

    @JsonProperty
    private final Long messagesInCount;

    @JsonProperty
    private final Long bytesInCount;

    @JsonProperty
    private final Long bytesOutCount;

    @JsonProperty
    private final Map<Integer, WrappedTopicPartitionMetrics> partitionMetrics;

    @JsonProperty
    private final Long retentionMs;

    @JsonProperty
    private TopicSummary topicSummary;

    private AggrTopicMetrics(String name,
                             Long messagesInCount,
                             Long bytesInCount,
                             Long bytesOutCount,
                             Map<Integer, WrappedTopicPartitionMetrics> partitionMetrics,
                             Long retentionMs,
                             TopicSummary topicSummary) {
        this.name = name;
        this.messagesInCount = messagesInCount;
        this.bytesInCount = bytesInCount;
        this.bytesOutCount = bytesOutCount;
        this.partitionMetrics = partitionMetrics;
        this.retentionMs = retentionMs;
        this.topicSummary = topicSummary;
    }

    public String name() {
        return name;
    }

    public Long messagesInCount() {
        return messagesInCount;
    }

    public Long bytesInCount() {
        return bytesInCount;
    }

    public Long bytesOutCount() {
        return bytesOutCount;
    }

    public Map<Integer, WrappedTopicPartitionMetrics> partitionMetrics() {
        return partitionMetrics;
    }

    public Long retentionMs() { return retentionMs;}

    public TopicSummary topicSummary() {
        return topicSummary;
    }

    // For jackson deserialization
    private AggrTopicMetrics() {
        this(null, null, null, null, null, null, null);
    }

    public static AggrTopicMetrics from(TopicInfo topicInfo,
                                        TopicMetrics topicMetrics,
                                        Collection<ProducerMetrics> producerMetrics,
                                        Long retentionMs,
                                        TopicSummary topicSummary,
                                        Collection<ConsumerGroupInfo> consumerGroups,
                                        SMMAuthorizer authorizer,
                                        SecurityContext securityContext) {
        String thisTopic = topicInfo.name();
        Long messagesInCount = extractMinMaxTimestampDeltaValue(topicMetrics.messagesInCount());
        Long bytesInCount = extractMinMaxTimestampDeltaValue(topicMetrics.bytesInCount());
        Long bytesOutCount = extractMinMaxTimestampDeltaValue(topicMetrics.bytesOutCount());

        // topicInfo.partitions() can be a sequence of partitions with respective index but that assumption
        // can be broken whenever somebody is building topicInfo (especially through json).
        final Map<Integer, TopicPartitionInfo> partitions = new HashMap<>();
        topicInfo.partitions().forEach(x -> partitions.put(x.partition(), x));

        // collecting partition metrics
        Map<Integer, WrappedTopicPartitionMetrics> partitionMetricsMap = new HashMap<>();
        for (Map.Entry<Integer, TopicPartitionInfo> partitionInfoEntry : partitions.entrySet()) {
            Integer partition = partitionInfoEntry.getKey();
            TopicPartitionInfo partitionInfo = partitionInfoEntry.getValue();
            TopicPartitionMetrics topicPartitionMetrics = topicMetrics.partitionMetrics()
                    .getOrDefault(partition, TopicPartitionMetrics.EMPTY);
            WrappedTopicPartitionMetrics wrappedPartitionMetrics = new WrappedTopicPartitionMetrics();
            wrappedPartitionMetrics.aggrTopicPartitionMetrics = AggrTopicPartitionMetrics.from(partitionInfo,
                    topicPartitionMetrics);
            partitionMetricsMap.put(partition, wrappedPartitionMetrics);
        }

        // finds all the producerId for the given topic
        for (ProducerMetrics prodMetrics : producerMetrics) {
            for (Map.Entry<String, Map<Integer, Map<Long, Long>>> entry : prodMetrics.outMessagesCount().entrySet()) {
                String currentTopic = entry.getKey();
                if (thisTopic.equals(currentTopic)) {
                    for (Map.Entry<Integer, Map<Long, Long>> pe : entry.getValue().entrySet()) {
                        Integer currentPartition = pe.getKey();
                        WrappedTopicPartitionMetrics wrappedPartitionMetrics = partitionMetricsMap.get(currentPartition);
                        wrappedPartitionMetrics.producerIdToOutMessagesCount.put(prodMetrics.clientId(),
                                                                             extractMinMaxTimestampDeltaValue(
                                                                                     pe.getValue()));
                    }
                }
            }
        }

        // finds all the consumerId for the given topic
        for (ConsumerGroupInfo groupInfo : consumerGroups) {
            if (!SecurityUtil.authorizeGroupDescribe(authorizer, securityContext, groupInfo.id()))
                continue;
            for (Map.Entry<String, Map<Integer, PartitionAssignment>> entry : groupInfo.topicPartitionAssignments()
                                                                                       .entrySet()) {
                String currentTopic = entry.getKey();
                if (thisTopic.equals(currentTopic)) {
                    for (Map.Entry<Integer, PartitionAssignment> pe : entry.getValue().entrySet()) {
                        Integer currentPartition = pe.getKey();
                        WrappedTopicPartitionMetrics wrappedPartitionMetrics = partitionMetricsMap.get(currentPartition);
                        wrappedPartitionMetrics.consumerGroupIdToLag.put(groupInfo.id(), pe.getValue().lag());
                    }
                }
            }
        }

        return new AggrTopicMetrics(thisTopic, messagesInCount, bytesInCount, bytesOutCount, partitionMetricsMap,
                                    retentionMs, topicSummary);
    }

    @Override
    public String toString() {
        return "AggrTopicMetrics{" +
               "name=" + name +
               ", messagesInCount=" + messagesInCount +
               ", bytesInCount=" + bytesInCount +
               ", bytesOutCount=" + bytesOutCount +
               ", partitionMetrics=" + partitionMetrics +
               ", retentionMs=" + retentionMs +
               ", topicSummary=" + topicSummary +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggrTopicMetrics that = (AggrTopicMetrics) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(messagesInCount, that.messagesInCount) &&
               Objects.equals(bytesInCount, that.bytesInCount) &&
               Objects.equals(bytesOutCount, that.bytesOutCount) &&
               Objects.equals(partitionMetrics, that.partitionMetrics) &&
               Objects.equals(retentionMs, that.retentionMs) &&
               Objects.equals(topicSummary, that.topicSummary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, messagesInCount, bytesInCount, bytesOutCount, partitionMetrics, retentionMs, topicSummary);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WrappedTopicPartitionMetrics {

        @JsonProperty
        private AggrTopicPartitionMetrics aggrTopicPartitionMetrics;

        @JsonProperty
        private final Map<String, Long> producerIdToOutMessagesCount = new HashMap<>();

        @JsonProperty
        private final Map<String, Long> consumerGroupIdToLag = new HashMap<>();

        private WrappedTopicPartitionMetrics() {
        }

        public AggrTopicPartitionMetrics aggrTopicPartitionMetrics() {
            return aggrTopicPartitionMetrics;
        }

        public Map<String, Long> producerIdToOutMessagesCount() {
            return producerIdToOutMessagesCount;
        }

        public Map<String, Long> consumerGroupIdToLag() {
            return consumerGroupIdToLag;
        }

        @Override
        public String toString() {
            return "WrappedTopicPartitionMetrics{" +
                   "aggrTopicPartitionMetrics=" + aggrTopicPartitionMetrics +
                   ", producerIdToOutMessagesCount=" + producerIdToOutMessagesCount +
                   ", consumerGroupIdToLag=" + consumerGroupIdToLag +
                   '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrappedTopicPartitionMetrics that = (WrappedTopicPartitionMetrics) o;
            return Objects.equals(aggrTopicPartitionMetrics, that.aggrTopicPartitionMetrics) &&
                   Objects.equals(producerIdToOutMessagesCount, that.producerIdToOutMessagesCount) &&
                   Objects.equals(consumerGroupIdToLag, that.consumerGroupIdToLag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggrTopicPartitionMetrics, producerIdToOutMessagesCount, consumerGroupIdToLag);
        }
    }
}
