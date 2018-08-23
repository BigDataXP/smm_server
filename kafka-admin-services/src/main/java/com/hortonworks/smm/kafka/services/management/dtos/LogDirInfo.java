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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogDirInfo {

    @JsonProperty
    private Errors error;

    @JsonProperty
    private Map<TopicPartition, ReplicaInfo> replicaInfos;

    private LogDirInfo() {
    }

    private LogDirInfo(Map<TopicPartition, ReplicaInfo> replicaInfos, Errors error) {
        this.error = error;
        this.replicaInfos = replicaInfos;
    }

    public static LogDirInfo from(DescribeLogDirsResponse.LogDirInfo logDirInfo) {
        return new LogDirInfo(logDirInfo.replicaInfos
                                      .entrySet()
                                      .stream()
                                      .collect(Collectors.toMap(e -> new TopicPartition(e.getKey().topic(),
                                                                                        e.getKey().partition()),
                                                                e -> ReplicaInfo.from(e.getValue()))),
                              logDirInfo.error);
    }

    public Errors errors() {
        return this.error;
    }

    public Map<TopicPartition, ReplicaInfo> replicaInfos() {
        return this.replicaInfos;
    }

    @Override
    public String toString() {
        return "LogDirInfo{" +
                "error=" + error +
                ", replicaInfos=" + replicaInfos +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogDirInfo that = (LogDirInfo) o;
        return error == that.error &&
                Objects.equals(replicaInfos, that.replicaInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, replicaInfos);
    }
}
