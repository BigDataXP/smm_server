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
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartition;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.metric.MetricUtils;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;

import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggrConsumerMetrics {

    @JsonProperty
    private final ConsumerGroupInfo consumerGroupInfo;

    @JsonProperty
    private final Map<String, Map<Integer, WrappedConsumerMetrics>> wrappedPartitionMetrics;

    private AggrConsumerMetrics(ConsumerGroupInfo consumerGroupInfo,
                                Map<String, Map<Integer, WrappedConsumerMetrics>> wrappedPartitionMetrics) {
        this.consumerGroupInfo = consumerGroupInfo;
        this.wrappedPartitionMetrics = wrappedPartitionMetrics;
    }

    public ConsumerGroupInfo consumerGroupInfo() {
        return consumerGroupInfo;
    }

    public Map<String, Map<Integer, WrappedConsumerMetrics>> wrappedPartitionMetrics() {
        return wrappedPartitionMetrics;
    }

    public static List<AggrConsumerMetrics> from(Collection<ConsumerGroupInfo> groupInfos,
                                                 MetricsService metricsService,
                                                 Map<String, TopicInfo> topicInfos,
                                                 boolean requireTimelineMetrics,
                                                 TimeSpan timeSpan,
                                                 Collection<ProducerMetrics> producerMetrics,
                                                 SMMAuthorizer authorizer,
                                                 SecurityContext securityContext) {
        Map<String, ConsumerGroupMetrics> groupMetricsById = null;
        if (requireTimelineMetrics) {
            List<String> groupIds = groupInfos.stream().map(x -> x.id()).collect(Collectors.toList());
            groupMetricsById = ConsumerGroupMetrics.from(groupIds, timeSpan, metricsService);
        }
        return from(groupMetricsById, groupInfos, metricsService, topicInfos, requireTimelineMetrics, timeSpan,
                producerMetrics, authorizer, securityContext);
    }

    public static AggrConsumerMetrics from(ConsumerGroupInfo groupInfo,
                                           MetricsService metricsService,
                                           Map<String, TopicInfo> topicInfos,
                                           boolean requireTimelineMetrics,
                                           TimeSpan timeSpan,
                                           Collection<ProducerMetrics> producerMetrics,
                                           SMMAuthorizer authorizer,
                                           SecurityContext securityContext) {
        Map<String, ConsumerGroupMetrics> groupMetricsById = null;
        if (requireTimelineMetrics) {
            groupMetricsById = new HashMap<>();
            groupMetricsById.put(groupInfo.id(), ConsumerGroupMetrics.from(groupInfo.id(), timeSpan, metricsService));
        }

        List<AggrConsumerMetrics> aggrConsumerMetrics = from(groupMetricsById,
                Collections.singletonList(groupInfo),
                metricsService,
                topicInfos,
                requireTimelineMetrics,
                timeSpan,
                producerMetrics,
                authorizer,
                securityContext);
        return aggrConsumerMetrics.iterator().next();
    }

    private static List<AggrConsumerMetrics> from(Map<String, ConsumerGroupMetrics> groupMetricsById,
                                                  Collection<ConsumerGroupInfo> groupInfos,
                                                  MetricsService metricsService,
                                                  Map<String, TopicInfo> topicInfos,
                                                  boolean requireTimelineMetrics,
                                                  TimeSpan timeSpan,
                                                  Collection<ProducerMetrics> producerMetrics,
                                                  SMMAuthorizer authorizer,
                                                  SecurityContext securityContext) {
        // Topic metrics is cached as different consumer groups can subscribe to the same topics.
        // And, we are using WILD_CARD query to fetch all the topic-partition metrics from the Metrics cache.
        Map<String, TopicMetrics> topicMetricsMap = new HashMap<>();
        List<AggrConsumerMetrics> aggrConsumerMetrics = new ArrayList<>();
        for (ConsumerGroupInfo groupInfo : groupInfos) {
            if (!SecurityUtil.authorizeGroupDescribe(authorizer, securityContext, groupInfo.id()))
                continue;

            final Map<String, Map<Integer, WrappedConsumerMetrics>> wrappedPartitionMetrics = new HashMap<>();
            final Set<String> topics = groupInfo.topicPartitionAssignments().keySet();

            for (Map.Entry<String, Map<Integer, PartitionAssignment>> topicEntry : groupInfo.topicPartitionAssignments()
                    .entrySet()) {

                String topic = topicEntry.getKey();
                for (Map.Entry<Integer, PartitionAssignment> partitionEntry : topicEntry.getValue().entrySet()) {
                    wrappedPartitionMetrics.computeIfAbsent(topic, x -> new HashMap<>())
                            .put(partitionEntry.getKey(),
                                    new WrappedConsumerMetrics());
                }
            }

            for (String topic : topics) {
                TopicInfo topicInfo = topicInfos.get(topic);
                if (topicInfo != null) {
                    TopicMetrics topicMetrics = topicMetricsMap.computeIfAbsent(topic,
                            x -> TopicMetrics.from(x, metricsService, timeSpan));

                    // topicInfo.partitions() can be a sequence of partitions with respective index but that assumption
                    // can be broken whenever somebody is building topicInfo (especially through json).
                    final Map<Integer, TopicPartitionInfo> partitions = new HashMap<>();
                    topicInfo.partitions().forEach(x -> partitions.put(x.partition(), x));

                    for (Map.Entry<Integer, TopicPartitionMetrics> entry : topicMetrics.partitionMetrics().entrySet()) {
                        Integer partition = entry.getKey();
                        WrappedConsumerMetrics wrappedMetrics = wrappedPartitionMetrics.get(topic).get(partition);
                        if (wrappedMetrics != null) {
                            TopicPartitionInfo partitionInfo = partitions.get(partition);
                            if (partitionInfo != null) {
                                wrappedMetrics.partitionMetrics
                                        = AggrTopicPartitionMetrics.from(partitionInfo, entry.getValue());
                            }

                            if (requireTimelineMetrics) {
                                ConsumerGroupMetrics consumerGroupMetrics = groupMetricsById.get(groupInfo.id());
                                if (consumerGroupMetrics != null) {
                                    wrappedMetrics.groupPartitionMetrics =
                                            consumerGroupMetrics.topicPartitionMetrics()
                                                    //topic may be newly assigned and metrics may not exist for that
                                                    //in that time span
                                                    .getOrDefault(topic, Collections.emptyMap())
                                                    .get(partition);
                                }
                            }
                        }
                    }
                }
            }

            // transform topic-partition with prodClientId-metrics.
            Map<TopicPartition, Map<String, Long>> tpProdMetrics = new HashMap<>();
            for (ProducerMetrics producerMetric : producerMetrics) {
                for (Map.Entry<String, Map<Integer, Map<Long, Long>>> msgCountEntry : producerMetric.outMessagesCount()
                        .entrySet()) {
                    String topic = msgCountEntry.getKey();

                    if (!SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topic))
                        continue;

                    for (Map.Entry<Integer, Map<Long, Long>> partitionMsgCountEntry : msgCountEntry.getValue().entrySet()) {
                        Integer partition = partitionMsgCountEntry.getKey();
                        tpProdMetrics.computeIfAbsent(new TopicPartition(topic, partition), x -> new HashMap<>())
                                .put(producerMetric.clientId(),
                                        MetricUtils.extractMinMaxTimestampDeltaValue(partitionMsgCountEntry.getValue()));
                    }
                }
            }

            for (Map.Entry<String, Map<Integer, WrappedConsumerMetrics>> topicEntry : wrappedPartitionMetrics.entrySet()) {
                String topic = topicEntry.getKey();
                for (Map.Entry<Integer, WrappedConsumerMetrics> partitionEntry : topicEntry.getValue().entrySet()) {
                    Integer partition = partitionEntry.getKey();
                    WrappedConsumerMetrics wrappedMetrics = partitionEntry.getValue();

                    wrappedMetrics.producerIdToOutMessagesCount = tpProdMetrics.get(new TopicPartition(topic, partition));
                }
            }

            aggrConsumerMetrics.add(new AggrConsumerMetrics(groupInfo, wrappedPartitionMetrics));
        }
        return aggrConsumerMetrics;
    }

    @Override
    public String toString() {
        return "AggrConsumerMetrics{" +
               "consumerGroupInfo=" + consumerGroupInfo +
               ", wrappedPartitionMetrics=" + wrappedPartitionMetrics +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggrConsumerMetrics that = (AggrConsumerMetrics) o;
        return Objects.equals(consumerGroupInfo, that.consumerGroupInfo) &&
               Objects.equals(wrappedPartitionMetrics, that.wrappedPartitionMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupInfo, wrappedPartitionMetrics);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WrappedConsumerMetrics {
        @JsonProperty
        private AggrTopicPartitionMetrics partitionMetrics;

        @JsonProperty
        private Map<String, Long> producerIdToOutMessagesCount = new HashMap<>();

        @JsonProperty
        private ConsumerGroupPartitionMetrics groupPartitionMetrics;

        private WrappedConsumerMetrics() {
        }

        public AggrTopicPartitionMetrics partitionMetrics() {
            return partitionMetrics;
        }

        public Map<String, Long> producerIdToOutMessagesCount() {
            return producerIdToOutMessagesCount;
        }

        public ConsumerGroupPartitionMetrics consumerGroupPartitionMetrics() {
            return groupPartitionMetrics;
        }

        @Override
        public String toString() {
            return "WrappedConsumerMetrics{" +
                   "partitionMetrics=" + partitionMetrics +
                   ", producerIdToOutMessagesCount=" + producerIdToOutMessagesCount +
                    ", groupPartitionMetrics=" + groupPartitionMetrics +
                   '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrappedConsumerMetrics that = (WrappedConsumerMetrics) o;
            return Objects.equals(partitionMetrics, that.partitionMetrics) &&
                   Objects.equals(producerIdToOutMessagesCount, that.producerIdToOutMessagesCount) &&
                    Objects.equals(groupPartitionMetrics, that.groupPartitionMetrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionMetrics, producerIdToOutMessagesCount, groupPartitionMetrics);
        }
    }

}
