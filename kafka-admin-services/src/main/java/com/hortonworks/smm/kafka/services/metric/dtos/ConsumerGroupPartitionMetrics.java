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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupPartitionMetrics {

    @JsonProperty
    private Map<Long, Long> lag;

    @JsonProperty
    private Map<Long, Double> lagRate;

    @JsonProperty
    private Map<Long, Long> committedOffsets;

    @JsonProperty
    private Map<Long, Double> committedOffsetsRate;

    private ConsumerGroupPartitionMetrics() {
    }

    public ConsumerGroupPartitionMetrics(Map<Long, Long> lag,
                                         Map<Long, Double> lagRate,
                                         Map<Long, Long> committedOffsets,
                                         Map<Long, Double> committedOffsetsRate) {
        this.lag = lag;
        this.lagRate = lagRate;
        this.committedOffsets = committedOffsets;
        this.committedOffsetsRate = committedOffsetsRate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerGroupPartitionMetrics that = (ConsumerGroupPartitionMetrics) o;
        return Objects.equals(lag, that.lag) &&
            Objects.equals(lagRate, that.lagRate) &&
            Objects.equals(committedOffsets, that.committedOffsets) &&
            Objects.equals(committedOffsetsRate, that.committedOffsetsRate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lag, lagRate, committedOffsets, committedOffsetsRate);
    }

    public static class Builder {
        private Map<Long, Long> lag = new HashMap<>();
        private Map<Long, Double> lagRate = new HashMap<>();
        private Map<Long, Long> committedOffsets = new HashMap<>();
        private Map<Long, Double> committedOffsetRate = new HashMap<>();

        public Builder addLag(Map<Long, Double> lag) {
            lag.forEach((k, v) -> this.lag.put(k, v.longValue()));
            return this;
        }

        public Builder addLagRate(Map<Long, Double> lagRate) {
            this.lagRate.putAll(lagRate);
            return this;
        }

        public Builder addCommittedOffsets(Map<Long, Double> committedOffsets) {
            committedOffsets.forEach((k, v) -> this.committedOffsets.put(k, v.longValue()));
            return this;
        }

        public Builder addCommittedOffsetRate(Map<Long, Double> committedOffsetRate) {
            this.committedOffsetRate.putAll(committedOffsetRate);
            return this;
        }

        public ConsumerGroupPartitionMetrics build() {
            return new ConsumerGroupPartitionMetrics(lag, lagRate, committedOffsets, committedOffsetRate);
        }

    }
}
