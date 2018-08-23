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
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import javax.ws.rs.core.SecurityContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMinMaxTimestampDeltaValue;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggrProducerMetrics {

    @JsonProperty
    private final String clientId;

    @JsonProperty
    private final Boolean active;

    @JsonProperty
    private final Long latestOutMessagesCount;

    @JsonProperty
    private final Map<Long, Long> outMessagesCount;

    @JsonProperty
    private final Map<String, Map<Integer, WrappedProducerMetrics>> wrappedPartitionMetrics;

    private AggrProducerMetrics(String clientId,
                                Boolean active,
                                Long latestOutMessagesCount,
                                Map<Long, Long> outMessagesCount,
                                Map<String, Map<Integer, WrappedProducerMetrics>> wrappedPartitionMetrics) {
        this.clientId = clientId;
        this.active = active;
        this.latestOutMessagesCount = latestOutMessagesCount;
        this.outMessagesCount = outMessagesCount;
        this.wrappedPartitionMetrics = wrappedPartitionMetrics;
    }

    public String clientId() {
        return clientId;
    }

    public Boolean active() {
        return active;
    }

    public Long latestOutMessagesCount() {
        return latestOutMessagesCount;
    }

    public Map<Long, Long> outMessagesCount() {
        return outMessagesCount;
    }

    public Map<String, Map<Integer, WrappedProducerMetrics>> wrappedPartitionMetrics() {
        return wrappedPartitionMetrics;
    }

    public static AggrProducerMetrics from(ProducerMetrics prodMetrics,
                                           MetricsService metricsService,
                                           Map<String, TopicInfo> topicInfos,
                                           TimeSpan timeSpan,
                                           Collection<ConsumerGroupInfo> consumerGroups,
                                           SMMAuthorizer authorizer, SecurityContext securityContext) {
        Long prodLatestOutMessagesCount = 0L;
        Map<Long, Long> prodOutMessagesCount = new HashMap<>();
        Map<String, Map<Integer, WrappedProducerMetrics>> wrappedPartitionMetrics = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Map<Long, Long>>> entry : prodMetrics.outMessagesCount().entrySet()) {
            for (Map.Entry<Integer, Map<Long, Long>> partitionEntry : entry.getValue().entrySet()) {
                WrappedProducerMetrics wrappedMetrics = new WrappedProducerMetrics();
                wrappedMetrics.latestOutMessagesCount = extractMinMaxTimestampDeltaValue(partitionEntry.getValue());
                prodLatestOutMessagesCount += wrappedMetrics.latestOutMessagesCount;
                partitionEntry.getValue().forEach((timestamp, currentOutMessagesCount) -> {
                    Long val = prodOutMessagesCount.computeIfAbsent(timestamp, x -> 0L);
                    prodOutMessagesCount.put(timestamp, val + currentOutMessagesCount);
                });
                wrappedPartitionMetrics.computeIfAbsent(entry.getKey(), x -> new HashMap<>())
                                       .put(partitionEntry.getKey(), wrappedMetrics);
            }
        }

        // While fetching from AMS, it's better to fetch topic metrics with wild-card instead of fetching individual
        // partition metrics which reduces the number of calls to AMS, so grouping the topic-partition w.r.t topic
        Set<String> topics = wrappedPartitionMetrics.keySet();
        for (String topic : topics) {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo != null) {
                TopicMetrics topicMetrics = TopicMetrics.from(topic, metricsService, timeSpan);

                // topicInfo.partitions() can be a sequence of partitions with respective index but that assumption
                // can be broken whenever somebody is building topicInfo (especially through json).
                final Map<Integer, TopicPartitionInfo> partitions = new HashMap<>();
                topicInfo.partitions().forEach( x-> partitions.put(x.partition(), x));

                for (Map.Entry<Integer, TopicPartitionMetrics> entry : topicMetrics.partitionMetrics().entrySet()) {
                    Integer partition = entry.getKey();
                    WrappedProducerMetrics wrappedMetrics = wrappedPartitionMetrics.get(topic).get(partition);
                    if (wrappedMetrics != null) {
                        TopicPartitionInfo partitionInfo = partitions.get(partition);

                        if (partitionInfo != null) {
                            wrappedMetrics.partitionMetrics
                                    = AggrTopicPartitionMetrics.from(partitionInfo, entry.getValue());
                        }
                    }
                }
            }
        }

        for (ConsumerGroupInfo groupInfo : consumerGroups) {
            for (Map.Entry<String, Map<Integer, PartitionAssignment>> topicEntry : groupInfo.topicPartitionAssignments()
                                                                                            .entrySet()) {
                if (!SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicEntry.getKey()))
                    continue;

                Map<Integer, WrappedProducerMetrics> partitionToMetrics =
                        wrappedPartitionMetrics.get(topicEntry.getKey());
                if (partitionToMetrics != null) {
                    for (Map.Entry<Integer, PartitionAssignment> partitionAssignmentEntry : topicEntry.getValue()
                                                                                                      .entrySet()) {
                        Integer partition = partitionAssignmentEntry.getKey();
                        WrappedProducerMetrics wrappedProducerMetrics = partitionToMetrics.get(partition);
                        if (wrappedProducerMetrics != null) {
                            PartitionAssignment partitionAssignment = partitionAssignmentEntry.getValue();
                            wrappedProducerMetrics.consumerGroupIdToLag.put(groupInfo.id(), partitionAssignment.lag());
                        }
                    }
                }
            }
        }

        return new AggrProducerMetrics(prodMetrics.clientId(), prodMetrics.active(), prodLatestOutMessagesCount,
                prodOutMessagesCount, wrappedPartitionMetrics);
    }

    @Override
    public String toString() {
        return "AggrProducerMetrics{" +
               "clientId='" + clientId + '\'' +
               ", wrappedPartitionMetrics=" + wrappedPartitionMetrics +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggrProducerMetrics that = (AggrProducerMetrics) o;
        return Objects.equals(clientId, that.clientId) &&
               Objects.equals(wrappedPartitionMetrics, that.wrappedPartitionMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, wrappedPartitionMetrics);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WrappedProducerMetrics {
        @JsonProperty
        private Long latestOutMessagesCount;

        @JsonProperty
        private AggrTopicPartitionMetrics partitionMetrics;

        @JsonProperty
        private final Map<String, Long> consumerGroupIdToLag = new HashMap<>();

        private WrappedProducerMetrics() {
        }

        public Long latestOutMessagesCount() {
            return latestOutMessagesCount;
        }

        public AggrTopicPartitionMetrics partitionMetrics() {
            return partitionMetrics;
        }

        public Map<String, Long> consumerGroupIdToLag() {
            return consumerGroupIdToLag;
        }

        @Override
        public String toString() {
            return "WrappedProducerMetrics{" +
                   "latestOutMessagesCount=" + latestOutMessagesCount +
                   ", partitionMetrics=" + partitionMetrics +
                   ", consumerGroupIdToLag=" + consumerGroupIdToLag +
                   '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrappedProducerMetrics that = (WrappedProducerMetrics) o;
            return Objects.equals(latestOutMessagesCount, that.latestOutMessagesCount) &&
                   Objects.equals(partitionMetrics, that.partitionMetrics) &&
                   Objects.equals(consumerGroupIdToLag, that.consumerGroupIdToLag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(latestOutMessagesCount, partitionMetrics, consumerGroupIdToLag);
        }
    }

}