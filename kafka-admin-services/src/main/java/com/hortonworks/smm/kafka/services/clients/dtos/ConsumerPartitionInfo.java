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
package com.hortonworks.smm.kafka.services.clients.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerPartitionInfo {

    @JsonProperty
    private String groupId;

    @JsonProperty
    private Map<String, Map<Integer, ConsumerPartitionOffsetInfo>> topicPartitionOffsets;

    private ConsumerPartitionInfo() {
    }

    private ConsumerPartitionInfo(String groupId,
                                 Map<String, Map<Integer, ConsumerPartitionOffsetInfo>> topicPartitionOffsets) {
        this.groupId = groupId;
        this.topicPartitionOffsets = topicPartitionOffsets;
    }

    public String getGroupId() {
        return groupId;
    }

    public Map<String, Map<Integer, ConsumerPartitionOffsetInfo>> topicPartitionOffsets() {
        return topicPartitionOffsets;
    }

    @Override
    public String toString() {
        return "ConsumerPartitionInfo{" +
               "groupId='" + groupId + '\'' +
               ", topicPartitionOffsets=" + topicPartitionOffsets +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerPartitionInfo that = (ConsumerPartitionInfo) o;
        return Objects.equals(groupId, that.groupId) &&
               Objects.equals(topicPartitionOffsets, that.topicPartitionOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicPartitionOffsets);
    }

    public static class Builder {
        private final String groupId;

        private Map<String, Map<Integer, ConsumerPartitionOffsetInfo>> topicPartitionOffsets = new HashMap<>();

        public Builder(String groupId) {
            this.groupId = groupId;
        }

        public Builder addTopicPartitionLag(String topic, Integer partition, Long offset, Long lag, Long commitTimestamp) {
            topicPartitionOffsets.computeIfAbsent(topic, x -> new HashMap<>())
                                 .put(partition, new ConsumerPartitionOffsetInfo(offset, lag, commitTimestamp));
            return this;
        }

        public ConsumerPartitionInfo build() {
            return new ConsumerPartitionInfo(groupId, topicPartitionOffsets);
        }

    }

}
