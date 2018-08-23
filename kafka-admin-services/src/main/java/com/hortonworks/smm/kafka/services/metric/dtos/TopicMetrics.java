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
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicMetrics {

    @JsonProperty
    private Map<Long, Long> messagesInCount;

    @JsonProperty
    private Map<Long, Long> bytesInCount;

    @JsonProperty
    private Map<Long, Long> bytesOutCount;

    @JsonProperty
    private Map<Integer, TopicPartitionMetrics> partitionMetrics;

    private TopicMetrics() {
    }

    private TopicMetrics(Map<Long, Long> messagesInCount,
                         Map<Long, Long> bytesInCount,
                         Map<Long, Long> bytesOutCount,
                         Map<Integer, TopicPartitionMetrics> partitionMetrics) {
        this.messagesInCount = messagesInCount;
        this.bytesInCount = bytesInCount;
        this.bytesOutCount = bytesOutCount;
        this.partitionMetrics = partitionMetrics;
    }

    public Map<Long, Long> messagesInCount() {
        return messagesInCount;
    }

    public Map<Long, Long> bytesInCount() {
        return bytesInCount;
    }

    public Map<Long, Long> bytesOutCount() {
        return bytesOutCount;
    }

    public Map<Integer, TopicPartitionMetrics> partitionMetrics() {
        return partitionMetrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicMetrics that = (TopicMetrics) o;
        return Objects.equals(messagesInCount, that.messagesInCount) &&
               Objects.equals(bytesInCount, that.bytesInCount) &&
               Objects.equals(bytesOutCount, that.bytesOutCount) &&
               Objects.equals(partitionMetrics, that.partitionMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messagesInCount, bytesInCount, bytesOutCount, partitionMetrics);
    }

    public static TopicMetrics from(String topic, MetricsService metricsService, TimeSpan timeSpan) {
        return from(metricsService.getTopicMessagesInCount(timeSpan, topic),
                metricsService.getTopicBytesInCount(timeSpan, topic),
                metricsService.getTopicBytesOutCount(timeSpan, topic),
                metricsService.getTopicPartitionMessagesInCount(timeSpan, topic),
                metricsService.getTopicPartitionBytesInCount(timeSpan, topic),
                metricsService.getTopicPartitionBytesOutCount(timeSpan, topic));
    }

    public static TopicMetrics from(Map<MetricDescriptor, Map<Long, Double>> messagesInCount,
                                    Map<MetricDescriptor, Map<Long, Double>> bytesInCount,
                                    Map<MetricDescriptor, Map<Long, Double>> bytesOutCount,
                                    Map<MetricDescriptor, Map<Long, Double>> partitionMessagesInCount,
                                    Map<MetricDescriptor, Map<Long, Double>> partitionBytesInCount,
                                    Map<MetricDescriptor, Map<Long, Double>> partitionBytesOutCount) {

        return new TopicMetrics(extractValue(messagesInCount),
                                extractValue(bytesInCount),
                                extractValue(bytesOutCount),
                                convertToPartitionMetrics(partitionMessagesInCount, partitionBytesInCount,
                                                          partitionBytesOutCount));
    }

    private static Map<Long, Long> extractValue(Map<MetricDescriptor, Map<Long, Double>> map) {
        if (map == null || map.isEmpty()) {
            return Collections.emptyMap();
        } else {
            Map<Long, Long> result = new HashMap<>();
            Map<Long, Double> valueMap = map.entrySet().iterator().next().getValue();
            valueMap.forEach((k, v) -> result.put(k, v.longValue()));
            return result;
        }
    }

    private static Map<Integer, TopicPartitionMetrics> convertToPartitionMetrics(
            Map<MetricDescriptor, Map<Long, Double>> partitionMessagesInCount,
            Map<MetricDescriptor, Map<Long, Double>> partitionBytesInCount,
            Map<MetricDescriptor, Map<Long, Double>> partitionBytesOutCount) {

        HashMap<Integer, TopicPartitionMetrics.Builder> partitionMetricsBuilders = new HashMap<>();

        if (partitionMessagesInCount != null) {
            for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionMessagesInCount.entrySet()) {
                partitionMetricsBuilders
                        .computeIfAbsent(extractPartition(entry), integer -> new TopicPartitionMetrics.Builder())
                        .addMessagesInCount(entry.getValue());
            }
        }

        if (partitionBytesInCount != null) {
            for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionBytesInCount.entrySet()) {
                partitionMetricsBuilders
                        .computeIfAbsent(extractPartition(entry), integer -> new TopicPartitionMetrics.Builder())
                        .addBytesInCount(entry.getValue());
            }
        }

        if (partitionBytesOutCount != null) {
            for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : partitionBytesOutCount.entrySet()) {
                partitionMetricsBuilders
                        .computeIfAbsent(extractPartition(entry), integer -> new TopicPartitionMetrics.Builder())
                        .addBytesOutCount(entry.getValue());
            }
        }

        HashMap<Integer, TopicPartitionMetrics> result = new HashMap<>(partitionMetricsBuilders.size());
        partitionMetricsBuilders.forEach((k, v) -> result.put(k, v.build()));

        return result;
    }

    private static int extractPartition(Map.Entry<MetricDescriptor, Map<Long, Double>> entry) {
        return Integer.parseInt(entry.getKey().queryTags().get("partition"));
    }
}
