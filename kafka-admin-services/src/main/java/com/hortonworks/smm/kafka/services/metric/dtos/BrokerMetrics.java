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
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerMetrics {

    @JsonProperty
    private Map<Long, Long> bytesInCount;

    @JsonProperty
    private Map<Long, Long> bytesOutCount;

    @JsonProperty
    private Map<Long, Long> messagesInCount;

    @JsonProperty
    private Map<Long, Double> cpuIdle;

    @JsonProperty
    private Map<Long, Double> loadFive;

    @JsonProperty
    private Map<Long, Double> memFreePercent;

    @JsonProperty
    private Map<Long, Double> diskPercent;

    @JsonProperty
    private Map<Long, Double> diskWriteBps;

    @JsonProperty
    private Map<Long, Double> diskReadBps;

    private BrokerMetrics() {
    }

    private BrokerMetrics(Map<Long, Long> bytesInCount,
                          Map<Long, Long> bytesOutCount,
                          Map<Long, Long> messagesInCount,
                          Map<Long, Double> cpuIdle,
                          Map<Long, Double> loadFive,
                          Map<Long, Double> memFreePercent,
                          Map<Long, Double> diskPercent,
                          Map<Long, Double> diskWriteBps,
                          Map<Long, Double> diskReadBps) {
        this.bytesInCount = bytesInCount;
        this.bytesOutCount = bytesOutCount;
        this.messagesInCount = messagesInCount;
        this.cpuIdle = cpuIdle;
        this.loadFive = loadFive;
        this.memFreePercent = memFreePercent;
        this.diskPercent = diskPercent;
        this.diskWriteBps = diskWriteBps;
        this.diskReadBps = diskReadBps;
    }

    public Map<Long, Long> bytesInCount() {
        return bytesInCount;
    }

    public Map<Long, Long> bytesOutCount() {
        return bytesOutCount;
    }

    public Map<Long, Long> messagesInCount() {
        return messagesInCount;
    }


    public Map<Long, Double> cpuIdle() {
        return cpuIdle;
    }

    public Map<Long, Double> loadFive() {
        return loadFive;
    }

    public Map<Long, Double> memFreePercent() {
        return memFreePercent;
    }

    public Map<Long, Double> diskPercent() {
        return diskPercent;
    }

    public Map<Long, Double> diskWriteBps() {
        return diskWriteBps;
    }

    public Map<Long, Double> diskReadBps() {
        return diskReadBps;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerMetrics that = (BrokerMetrics) o;
        return Objects.equals(bytesInCount, that.bytesInCount) &&
                Objects.equals(bytesOutCount, that.bytesOutCount) &&
                Objects.equals(messagesInCount, that.messagesInCount) &&
                Objects.equals(cpuIdle, that.cpuIdle) &&
                Objects.equals(loadFive, that.loadFive) &&
                Objects.equals(memFreePercent, that.memFreePercent) &&
                Objects.equals(diskPercent, that.diskPercent) &&
                Objects.equals(diskWriteBps, that.diskWriteBps) &&
                Objects.equals(diskReadBps, that.diskReadBps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesInCount, bytesOutCount, messagesInCount, cpuIdle,
                            loadFive, memFreePercent, diskPercent, diskWriteBps, diskReadBps);
    }

    public static BrokerMetrics from(BrokerNode node, MetricsService metricsService, TimeSpan timeSpan) {
        return from(metricsService.getBrokerBytesInCount(timeSpan, node),
                metricsService.getBrokerBytesOutCount(timeSpan, node),
                metricsService.getBrokerMessagesInCount(timeSpan, node),
                metricsService.getCpuIdleMetrics(timeSpan, node),
                metricsService.getLoadFiveMetrics(timeSpan, node),
                metricsService.getMemFreePercentMetrics(timeSpan, node),
                metricsService.getDiskPercentMetrics(timeSpan, node),
                metricsService.getDiskWriteBpsMetrics(timeSpan, node),
                metricsService.getDiskReadBpsMetrics(timeSpan, node));
    }

    public static BrokerMetrics from(Map<MetricDescriptor, Map<Long, Double>> bytesInCount,
                                     Map<MetricDescriptor, Map<Long, Double>> bytesOutCount,
                                     Map<MetricDescriptor, Map<Long, Double>> messagesInCount,
                                     Map<MetricDescriptor, Map<Long, Double>> cpuIdleMetrics,
                                     Map<MetricDescriptor, Map<Long, Double>> loadFiveMetrics,
                                     Map<MetricDescriptor, Map<Long, Double>> memFreePercentMetrics,
                                     Map<MetricDescriptor, Map<Long, Double>> diskPercentMetrics,
                                     Map<MetricDescriptor, Map<Long, Double>> diskWriteBpsMetrics,
                                     Map<MetricDescriptor, Map<Long, Double>> diskReadBpsMetrics) {

        return new BrokerMetrics(extractValueAsLong(bytesInCount),
                                 extractValueAsLong(bytesOutCount),
                                 extractValueAsLong(messagesInCount),
                                 extractValue(cpuIdleMetrics),
                                 extractValue(loadFiveMetrics),
                                 extractValue(memFreePercentMetrics),
                                 extractValue(diskPercentMetrics),
                                 extractValue(diskWriteBpsMetrics),
                                 extractValue(diskReadBpsMetrics));
    }

    private static Map<Long, Double> extractValue(Map<MetricDescriptor, Map<Long, Double>> valueMap) {
        if (valueMap == null || valueMap.isEmpty())
            return Collections.emptyMap();
        else
            return valueMap.entrySet().iterator().next().getValue();
    }

    private static Map<Long, Long> extractValueAsLong(Map<MetricDescriptor, Map<Long, Double>> map) {
        Map<Long, Long> result = new HashMap<>();
        extractValue(map).forEach((k, v) -> result.put(k, v.longValue()));
        return result;
    }
}
