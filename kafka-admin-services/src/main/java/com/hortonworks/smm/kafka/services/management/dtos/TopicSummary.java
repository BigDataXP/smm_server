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

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicSummary {

    @JsonProperty
    private Boolean internal;

    @JsonProperty
    private short numOfReplicas;

    @JsonProperty
    private int numOfPartitions;

    @JsonProperty
    private int numOfBrokersForTopic;

    @JsonProperty
    private double preferredReplicasPercent;

    @JsonProperty
    private double underReplicatedPercent;

    // For JSON deserialization
    private TopicSummary() {}

    public TopicSummary(short numOfReplicas, int numOfPartitions, int numOfBrokersForTopic, double preferredReplicasPercent, double underReplicatedPercent, boolean internal) {
        this.numOfReplicas = numOfReplicas;
        this.numOfPartitions = numOfPartitions;
        this.numOfBrokersForTopic = numOfBrokersForTopic;
        this.preferredReplicasPercent = preferredReplicasPercent;
        this.underReplicatedPercent = underReplicatedPercent;
        this.internal = internal;
    }

    public boolean isInternal() {
        return internal;
    }

    public short getNumOfReplicas() {
        return numOfReplicas;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public int getNumOfBrokersForTopic() {
        return numOfBrokersForTopic;
    }

    public double getPreferredReplicasPercent() {
        return preferredReplicasPercent;
    }

    public double getUnderReplicatedPercent() {
        return underReplicatedPercent;
    }

    @Override
    public String toString() {
        return "TopicSummary{" +
            "internal=" + internal +
            ", numOfReplicas=" + numOfReplicas +
            ", numOfPartitions=" + numOfPartitions +
            ", numOfBrokersForTopic=" + numOfBrokersForTopic +
            ", preferredReplicasPercent=" + preferredReplicasPercent +
            ", underReplicatedPercent=" + underReplicatedPercent +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicSummary that = (TopicSummary) o;
        return numOfReplicas == that.numOfReplicas &&
            numOfPartitions == that.numOfPartitions &&
            numOfBrokersForTopic == that.numOfBrokersForTopic &&
            Double.compare(that.preferredReplicasPercent, preferredReplicasPercent) == 0 &&
            Double.compare(that.underReplicatedPercent, underReplicatedPercent) == 0 &&
            Objects.equals(internal, that.internal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(internal, numOfReplicas, numOfPartitions, numOfBrokersForTopic, preferredReplicasPercent, underReplicatedPercent);
    }
}
