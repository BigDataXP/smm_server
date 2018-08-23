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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents the topicPartitionAssignments for a consumer group.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupInfo {

    @JsonProperty
    private String id;

    @JsonProperty
    private String state;

    @JsonProperty
    private boolean active;

    @JsonProperty
    private Map<String, Map<Integer, PartitionAssignment>> topicPartitionAssignments = Collections.emptyMap();

    private ConsumerGroupInfo() {
    }

    public ConsumerGroupInfo(String id, String state) {
        this (id, state, Collections.emptyMap(), false);
    }

    public ConsumerGroupInfo(String id,
                             String state,
                             Map<String, Map<Integer, PartitionAssignment>> topicPartitionAssignments,
                             boolean active) {
        Objects.requireNonNull(id, "id can not be null");
        Objects.requireNonNull(state, "state can not be null");
        Objects.requireNonNull(topicPartitionAssignments, "topicPartitionAssignments can not be null");

        this.id = id;
        this.state = state;
        this.topicPartitionAssignments = topicPartitionAssignments;
        this.active = active;
    }

    public String id() {
        return id;
    }

    public String state() {
        return state;
    }

    public boolean active() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Map<String, Map<Integer, PartitionAssignment>> topicPartitionAssignments() {
        return topicPartitionAssignments;
    }

    public void setTopicPartitionAssignments(Map<String, Map<Integer, PartitionAssignment>> topicPartitionAssignments) {
        this.topicPartitionAssignments = topicPartitionAssignments;
    }

    @Override
    public String toString() {
        return "ConsumerGroupInfo{" +
               "id='" + id + '\'' +
               ", state='" + state + '\'' +
               ", active=" + active +
               ", topicPartitionAssignments=" + topicPartitionAssignments +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroupInfo that = (ConsumerGroupInfo) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(state, that.state) &&
               Objects.equals(active, that.active) &&
               Objects.equals(topicPartitionAssignments, that.topicPartitionAssignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, active, topicPartitionAssignments);
    }
}
