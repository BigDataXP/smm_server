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
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;

import java.util.Objects;

import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMinMaxTimestampDeltaValue;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggrTopicPartitionMetrics {

    @JsonProperty
    private final TopicPartitionInfo partitionInfo;

    @JsonProperty
    private final Long messagesInCount;

    @JsonProperty
    private final Long bytesInCount;

    @JsonProperty
    private final Long bytesOutCount;

    // For jackson deserialization
    private AggrTopicPartitionMetrics() {
        this(null, null, null, null);
    }

    private AggrTopicPartitionMetrics(TopicPartitionInfo partitionInfo,
                                      Long messagesInCount,
                                      Long bytesInCount,
                                      Long bytesOutCount) {
        this.partitionInfo = partitionInfo;
        this.messagesInCount = messagesInCount;
        this.bytesInCount = bytesInCount;
        this.bytesOutCount = bytesOutCount;
    }

    public TopicPartitionInfo partitionInfo() {
        return partitionInfo;
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

    public static AggrTopicPartitionMetrics from(TopicPartitionInfo partitionInfo, TopicPartitionMetrics tpMetrics) {
        return new AggrTopicPartitionMetrics(partitionInfo,
                extractMinMaxTimestampDeltaValue(tpMetrics.messagesInCount()),
                extractMinMaxTimestampDeltaValue(tpMetrics.bytesInCount()),
                extractMinMaxTimestampDeltaValue(tpMetrics.bytesOutCount()));
    }

    @Override
    public String toString() {
        return "AggrTopicPartitionMetrics{" +
                "partitionInfo=" + partitionInfo +
                ", messagesInCount=" + messagesInCount +
                ", bytesInCount=" + bytesInCount +
                ", bytesOutCount=" + bytesOutCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggrTopicPartitionMetrics that = (AggrTopicPartitionMetrics) o;
        return Objects.equals(partitionInfo, that.partitionInfo) &&
                Objects.equals(messagesInCount, that.messagesInCount) &&
                Objects.equals(bytesInCount, that.bytesInCount) &&
                Objects.equals(bytesOutCount, that.bytesOutCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionInfo, messagesInCount, bytesInCount, bytesOutCount);
    }
}
