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
public class TopicPartitionMetrics {

    public static final TopicPartitionMetrics EMPTY = new TopicPartitionMetrics(Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap());

    @JsonProperty
    private Map<Long, Long> messagesInCount;

    @JsonProperty
    private Map<Long, Long> bytesInCount;

    @JsonProperty
    private Map<Long, Long> bytesOutCount;

    private TopicPartitionMetrics() {
    }

    private TopicPartitionMetrics(Map<Long, Long> messagesInCount,
                                  Map<Long, Long> bytesInCount,
                                  Map<Long, Long> bytesOutCount) {
        this.messagesInCount = messagesInCount;
        this.bytesInCount = bytesInCount;
        this.bytesOutCount = bytesOutCount;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartitionMetrics that = (TopicPartitionMetrics) o;
        return Objects.equals(messagesInCount, that.messagesInCount) &&
                Objects.equals(bytesInCount, that.bytesInCount) &&
                Objects.equals(bytesOutCount, that.bytesOutCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messagesInCount, bytesInCount, bytesOutCount);
    }

    public static TopicPartitionMetrics from(String topic, int partition, MetricsService metricsService, TimeSpan timeSpan) {
        return from(metricsService.getTopicPartitionMessagesInCount(timeSpan, topic, partition),
                metricsService.getTopicPartitionBytesInCount(timeSpan, topic, partition),
                metricsService.getTopicPartitionBytesOutCount(timeSpan, topic, partition));
    }

    public static TopicPartitionMetrics from(Map<MetricDescriptor, Map<Long, Double>> messagesInCountPerSec,
                                             Map<MetricDescriptor, Map<Long, Double>> bytesInCountPerSec,
                                             Map<MetricDescriptor, Map<Long, Double>> bytesOutCountPerSec) {
        return new TopicPartitionMetrics(extractValue(messagesInCountPerSec),
                                         extractValue(bytesInCountPerSec),
                                         extractValue(bytesOutCountPerSec));
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

    public static class Builder {
        private Map<Long, Long> messagesInCount = new HashMap<>();
        private Map<Long, Long> bytesInCount = new HashMap<>();
        private Map<Long, Long> bytesOutCount = new HashMap<>();

        public Builder addMessagesInCount(Map<Long, Double> messagesInCount) {
            messagesInCount.forEach((k, v) -> this.messagesInCount.put(k, v.longValue()));
            return this;
        }

        public Builder addBytesInCount(Map<Long, Double> bytesInCount) {
            bytesInCount.forEach((k, v) -> this.bytesInCount.put(k, v.longValue()));
            return this;
        }

        public Builder addBytesOutCount(Map<Long, Double> bytesOutCount) {
            bytesOutCount.forEach((k, v) -> this.bytesOutCount.put(k, v.longValue()));
            return this;
        }

        public TopicPartitionMetrics build() {
            return new TopicPartitionMetrics(messagesInCount, bytesInCount, bytesOutCount);
        }

    }
}
