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
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicInfo {

    @JsonProperty
    private String name;

    @JsonProperty
    private Boolean internal;

    @JsonProperty
    private List<TopicPartitionInfo> partitions;

    private TopicInfo() {
    }

    private TopicInfo(String name,
                      boolean internal,
                      List<TopicPartitionInfo> partitions) {
        this.name = name;
        this.internal = internal;
        this.partitions = partitions;
    }

    public static TopicInfo from(TopicDescription topicDescription) {
        return new TopicInfo(topicDescription.name(),
                             topicDescription.isInternal(),
                             topicDescription.partitions()
                                             .stream()
                                             .map(TopicPartitionInfo::from)
                                             .collect(Collectors.toList()));
    }

    public String name() {
        return name;
    }

    public boolean isInternal() {
        return internal;
    }

    public List<TopicPartitionInfo> partitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "TopicInfo{" +
                "name='" + name + '\'' +
                ", internal=" + internal +
                ", partitions=" + partitions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicInfo topicInfo = (TopicInfo) o;
        return internal == topicInfo.internal &&
                Objects.equals(name, topicInfo.name) &&
                Objects.equals(partitions, topicInfo.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, internal, partitions);
    }
}
