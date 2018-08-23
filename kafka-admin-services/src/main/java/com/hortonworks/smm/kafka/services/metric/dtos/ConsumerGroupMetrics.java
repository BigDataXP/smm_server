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
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartition;
import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupMetrics {

    @JsonProperty
    private Map<Long, Long> lag;

    @JsonProperty
    private Map<Long, Double> lagRate;

    @JsonProperty
    private Map<Long, Long> committedOffsets;

    @JsonProperty
    private Map<Long, Double> committedOffsetsRate;

    @JsonProperty
    private Map<String, Map<Integer, ConsumerGroupPartitionMetrics>> topicPartitionMetrics;

    private ConsumerGroupMetrics() {
    }

    private ConsumerGroupMetrics(Map<Long, Long> lag,
                                 Map<Long, Double> lagRate,
                                 Map<Long, Long> committedOffsets,
                                 Map<Long, Double> committedOffsetsRate,
                                 Map<String, Map<Integer, ConsumerGroupPartitionMetrics>> topicPartitionMetrics) {
        this.lag = lag;
        this.lagRate = lagRate;
        this.committedOffsets = committedOffsets;
        this.committedOffsetsRate = committedOffsetsRate;
        this.topicPartitionMetrics = topicPartitionMetrics;
    }

    public Map<String, Map<Integer, ConsumerGroupPartitionMetrics>> topicPartitionMetrics() {
        return topicPartitionMetrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerGroupMetrics that = (ConsumerGroupMetrics) o;
        return Objects.equals(lag, that.lag) &&
               Objects.equals(lagRate, that.lagRate) &&
               Objects.equals(committedOffsets, that.committedOffsets) &&
               Objects.equals(committedOffsetsRate, that.committedOffsetsRate) &&
               Objects.equals(topicPartitionMetrics, that.topicPartitionMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lag, lagRate, committedOffsets, committedOffsetsRate, topicPartitionMetrics);
    }

    public static Map<String, ConsumerGroupMetrics> from(List<String> groupIds,
                                                         TimeSpan timeSpan,
                                                         MetricsService metricsService) {
        return getConsumerGroupMetrics(MetricsService.WILD_CARD, groupIds, timeSpan, metricsService);
    }

    public static ConsumerGroupMetrics from(String groupId,
                                            TimeSpan timeSpan,
                                            MetricsService metricsService) {
        Map<String, ConsumerGroupMetrics> groupMetrics = getConsumerGroupMetrics(groupId,
                Collections.singletonList(groupId), timeSpan, metricsService);
        return groupMetrics.isEmpty() ? null : groupMetrics.entrySet().iterator().next().getValue();
    }

    private static Map<String, ConsumerGroupMetrics> getConsumerGroupMetrics(String key,
                                                                             List<String> groupIds,
                                                                             TimeSpan timeSpan,
                                                                             MetricsService metricsService) {
        Map<MetricDescriptor, Map<Long, Double>> partitionLag = metricsService.getPartitionLagForGroup(timeSpan, key);
        Map<MetricDescriptor, Map<Long, Double>> partitionLagRate = metricsService.getPartitionLagRateForGroup(timeSpan, key);
        Map<MetricDescriptor, Map<Long, Double>> partitionCommittedOffsets = metricsService.getPartitionCommittedOffsetsForGroup(timeSpan, key);
        Map<MetricDescriptor, Map<Long, Double>> partitionCommittedOffsetsRate = metricsService.getPartitionCommittedOffsetsRateForGroup(timeSpan, key);

        Map<String, Map<TopicPartition, ConsumerGroupPartitionMetrics.Builder>> groupPartitionMetricsBuilder = new HashMap<>();

        for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionLag.entrySet()) {
            String groupId = extractGroupId(entry);
            if (groupIds.contains(groupId)) {
                Optional<TopicPartition> tpOpt = buildTopicPartition(entry);
                tpOpt.ifPresent(tp -> groupPartitionMetricsBuilder
                        .computeIfAbsent(groupId, x -> new HashMap<>())
                        .computeIfAbsent(tp, x -> new ConsumerGroupPartitionMetrics.Builder())
                        .addLag(entry.getValue()));
            }
        }

        for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionLagRate.entrySet()) {
            String groupId = extractGroupId(entry);
            if (groupIds.contains(groupId)) {
                Optional<TopicPartition> tpOpt = buildTopicPartition(entry);
                tpOpt.ifPresent(tp -> groupPartitionMetricsBuilder
                        .computeIfAbsent(groupId, x -> new HashMap<>())
                        .computeIfAbsent(tp, x -> new ConsumerGroupPartitionMetrics.Builder())
                        .addLagRate(entry.getValue()));
            }
        }

        for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionCommittedOffsets.entrySet()) {
            String groupId = extractGroupId(entry);
            if (groupIds.contains(groupId)) {
                Optional<TopicPartition> tpOpt = buildTopicPartition(entry);
                tpOpt.ifPresent(tp -> groupPartitionMetricsBuilder
                        .computeIfAbsent(groupId, x -> new HashMap<>())
                        .computeIfAbsent(tp, x -> new ConsumerGroupPartitionMetrics.Builder())
                        .addCommittedOffsets(entry.getValue()));
            }
        }

        for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionCommittedOffsetsRate.entrySet()) {
            String groupId = extractGroupId(entry);
            if (groupIds.contains(groupId)) {
                Optional<TopicPartition> tpOpt = buildTopicPartition(entry);
                tpOpt.ifPresent(tp -> groupPartitionMetricsBuilder
                        .computeIfAbsent(groupId, x -> new HashMap<>())
                        .computeIfAbsent(tp, x -> new ConsumerGroupPartitionMetrics.Builder())
                        .addCommittedOffsetRate(entry.getValue()));
            }
        }

        Map<String, ConsumerGroupMetrics> groupMetrics = new HashMap<>();
        for (String groupId : groupPartitionMetricsBuilder.keySet()) {
            Map<TopicPartition, ConsumerGroupPartitionMetrics.Builder> partitionMetricsBuilder =
                    groupPartitionMetricsBuilder.get(groupId);

            Map<TopicPartition, ConsumerGroupPartitionMetrics> result = new HashMap<>(partitionMetricsBuilder.size());
            partitionMetricsBuilder.forEach((k, v) -> result.put(k, v.build()));
            Map<MetricDescriptor, Map<Long, Double>> groupLag = metricsService.getCumulativeLag(timeSpan, groupId);
            Map<MetricDescriptor, Map<Long, Double>> groupLagRate = metricsService.getCumulativeLagRate(timeSpan, groupId);
            Map<MetricDescriptor, Map<Long, Double>> groupCommittedOffsets = metricsService.getCumulativeCommittedOffsets(timeSpan, groupId);
            Map<MetricDescriptor, Map<Long, Double>> groupCommittedOffsetsRate = metricsService.getCumulativeCommittedOffsetRate(timeSpan, groupId);

            groupMetrics.put(groupId, new ConsumerGroupMetrics(extractValueAsLong(groupLag),
                    extractValue(groupLagRate),
                    extractValueAsLong(groupCommittedOffsets),
                    extractValue(groupCommittedOffsetsRate),
                    TopicPartition.transformTopicPartitionWithMap(result)));
        }
        return groupMetrics;
    }

    private static Optional<TopicPartition> buildTopicPartition(Map.Entry<MetricDescriptor, Map<Long, Double>> entry) {
        String topic = extractTopic(entry);
        Integer partition = extractPartition(entry);
        return topic != null && partition != null
               ? Optional.of(new TopicPartition(topic, partition)) : Optional.empty();
    }

    private static String extractTopic(Map.Entry<MetricDescriptor, Map<Long, Double>> entry) {
        return entry.getKey().queryTags().get("topic");
    }

    private static Integer extractPartition(Map.Entry<MetricDescriptor, Map<Long, Double>> entry) {
        String partition = entry.getKey().queryTags().get("partition");
        return partition != null ? Integer.valueOf(partition) : null;
    }

    private static String extractGroupId(Map.Entry<MetricDescriptor, Map<Long, Double>> entry) {
        return entry.getKey().queryTags().get(AbstractMetricDescriptorSupplier.CONSUMER_GROUP);
    }

    private static Map<Long, Double> extractValue(Map<MetricDescriptor, Map<Long, Double>> valueMap) {
        if (valueMap == null || valueMap.isEmpty()) {
            return Collections.emptyMap();
        } else {
            return valueMap.entrySet().iterator().next().getValue();
        }
    }

    private static Map<Long, Long> extractValueAsLong(Map<MetricDescriptor, Map<Long, Double>> map) {
        Map<Long, Long> result = new HashMap<>();
        extractValue(map).forEach((k, v) -> result.put(k, v.longValue()));
        return result;
    }
}
