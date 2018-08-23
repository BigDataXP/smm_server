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

import java.util.Objects;

/**
 * This class represents the partition assignment inside a consumer group.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionAssignment {

    public static final long DEFAULT_OFFSET = -1L;
    public static final String DEFAULT_CONSUMER_ID = "-";
    public static final String DEFAULT_CLIENT_ID = "-";
    public static final String DEFAULT_HOST = "-";

    @JsonProperty
    private Long lag;

    @JsonProperty
    private Long offset;

    @JsonProperty
    private Long logEndOffset;

    @JsonProperty
    private String consumerInstanceId;

    @JsonProperty
    private String clientId;

    @JsonProperty
    private String host;

    @JsonProperty
    private Long commitTimestamp;

    private PartitionAssignment() {
    }

    public PartitionAssignment(Long lag,
                               Long offset,
                               Long logEndOffset,
                               String consumerId,
                               String clientId,
                               String host,
                               Long commitTimestamp) {
        this.lag = lag;
        this.offset = offset;
        this.logEndOffset = logEndOffset;
        this.consumerInstanceId = consumerId;
        this.clientId = clientId;
        this.host = host;
        this.commitTimestamp = commitTimestamp;
    }

    public Long lag() {
        return lag;
    }

    public Long offset() {
        return offset;
    }

    public String clientId() {
        return clientId;
    }

    public String consumerInstanceId() {
        return consumerInstanceId;
    }

    public String host() {
        return host;
    }

    public Long logEndOffset() {
        return logEndOffset;
    }

    public Long commitTimestamp() {
        return commitTimestamp;
    }

    public void updateOffsets(Long leo, Long offset, Long lag, Long commitTimestamp) {
        this.logEndOffset = leo;
        this.offset = offset;
        this.lag = lag;
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public String toString() {
        return "PartitionAssignment{" +
                "lag=" + lag +
                ", offset=" + offset +
                ", logEndOffset=" + logEndOffset +
                ", consumerInstanceId='" + consumerInstanceId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", host='" + host + '\'' +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionAssignment that = (PartitionAssignment) o;
        return Objects.equals(lag, that.lag) &&
                Objects.equals(offset, that.offset) &&
                Objects.equals(logEndOffset, that.logEndOffset) &&
                Objects.equals(consumerInstanceId, that.consumerInstanceId) &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(host, that.host) &&
                Objects.equals(commitTimestamp, that.commitTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lag, offset, logEndOffset, consumerInstanceId, clientId, host, commitTimestamp);
    }
}
