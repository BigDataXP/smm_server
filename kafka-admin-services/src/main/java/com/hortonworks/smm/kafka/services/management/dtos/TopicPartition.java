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
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartition {

    @JsonProperty
    private String topic;

    @JsonProperty
    private int partition;

    private TopicPartition() {
    }

    public TopicPartition(String topic, int partition) {
        Validate.notBlank(topic, "topic name [%s] can not be null or empty", topic);
        Validate.inclusiveBetween(0, Integer.MAX_VALUE, partition, "partition must be an integer greater than 0");
        this.topic = topic;
        this.partition = partition;
    }

    public int partition() {
        return partition;
    }

    public String topic() {
        return topic;
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }

    public static TopicPartition from(String tpAsString) {
        int index = tpAsString.lastIndexOf("-");
        if (index == -1) {
            throw new IllegalArgumentException("tpAsString must be of the form `<topicName>-<partitionNumber>`");
        }

        return new TopicPartition(tpAsString.substring(0, index),
                                  Integer.parseInt(tpAsString.substring(index + 1, tpAsString.length())));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition &&
               Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, topic);
    }

    public static <T> Map<String, Map<Integer, T>> transformTopicPartitionWithMap(Map<TopicPartition, T> topicPartitionAssignments) {
        final HashMap<String, Map<Integer, T>> result = new HashMap<>();
        for (Map.Entry<TopicPartition, T> entry : topicPartitionAssignments.entrySet()) {
            result.computeIfAbsent(entry.getKey().topic(), e -> new HashMap<>())
                  .put(entry.getKey().partition(), entry.getValue());
        }
        return result;
    }

}
