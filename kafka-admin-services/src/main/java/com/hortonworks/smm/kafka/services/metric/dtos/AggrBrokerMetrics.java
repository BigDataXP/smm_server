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
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;

import java.util.Objects;

import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMaxTimestampValue;
import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMinMaxTimestampDeltaValue;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggrBrokerMetrics {

    @JsonProperty
    private final BrokerNode node;

    @JsonProperty
    private final Long throughput;

    @JsonProperty
    private final Long messageIn;

    @JsonProperty
    private final Double cpuIdle;

    @JsonProperty
    private final Double loadAvg;

    @JsonProperty
    private final Double memFreePercent;

    @JsonProperty
    private final Double diskPercent;

    @JsonProperty
    private final Double diskIo;

    // For jackson deserialization
    private AggrBrokerMetrics() {
        this(null, null, null, null, null, null, null, null);
    }

    private AggrBrokerMetrics(BrokerNode node,
                              Long throughput,
                              Long messageIn,
                              Double cpuIdle,
                              Double loadAvg,
                              Double memFreePercent,
                              Double diskPercent,
                              Double diskIo) {
        this.node = node;
        this.throughput = throughput;
        this.messageIn = messageIn;
        this.cpuIdle = cpuIdle;
        this.loadAvg = loadAvg;
        this.memFreePercent = memFreePercent;
        this.diskPercent = diskPercent;
        this.diskIo = diskIo;
    }

    public BrokerNode node() {
        return node;
    }

    public Long throughput() {
        return throughput;
    }

    public Long messageIn() { return messageIn;}

    public Double cpuIdle() {
        return cpuIdle;
    }

    public Double loadAvg() {
        return loadAvg;
    }

    public Double memFreePercent() {
        return memFreePercent;
    }

    public Double diskPercent() {
        return diskPercent;
    }

    public Double diskIo() {
        return diskIo;
    }


    public static AggrBrokerMetrics from(BrokerNode node,
                                         BrokerMetrics metrics) {
        Long throughput = extractMinMaxTimestampDeltaValue(metrics.bytesInCount()) +
                extractMinMaxTimestampDeltaValue(metrics.bytesOutCount());
        Long messageIn = extractMinMaxTimestampDeltaValue(metrics.messagesInCount());
        Double cpuIdle = extractMaxTimestampValue(metrics.cpuIdle());
        Double loadAvg = extractMaxTimestampValue(metrics.loadFive());
        Double memFreePercent = extractMaxTimestampValue(metrics.memFreePercent());
        Double diskPercent = extractMaxTimestampValue(metrics.diskPercent());
        Double diskIo = extractMaxTimestampValue(metrics.diskReadBps()) + extractMaxTimestampValue(metrics.diskWriteBps());
        return new AggrBrokerMetrics(node, throughput, messageIn, cpuIdle, loadAvg, memFreePercent, diskPercent, diskIo);
    }

    @Override
    public String toString() {
        return "AggrBrokerMetrics{" +
                "node=" + node +
                ", throughput=" + throughput +
                ", messageIn=" + messageIn +
                ", cpuIdle=" + cpuIdle +
                ", loadAvg=" + loadAvg +
                ", memFreePercent=" + memFreePercent +
                ", diskPercent=" + diskPercent +
                ", diskIo=" + diskIo +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggrBrokerMetrics that = (AggrBrokerMetrics) o;
        return Objects.equals(node, that.node) &&
                Objects.equals(throughput, that.throughput) &&
                Objects.equals(messageIn, that.messageIn) &&
                Objects.equals(cpuIdle, that.cpuIdle) &&
                Objects.equals(loadAvg, that.loadAvg) &&
                Objects.equals(memFreePercent, that.memFreePercent) &&
                Objects.equals(diskPercent, that.diskPercent) &&
                Objects.equals(diskIo, that.diskIo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, throughput, messageIn, cpuIdle, loadAvg, memFreePercent, diskPercent, diskIo);
    }
}
