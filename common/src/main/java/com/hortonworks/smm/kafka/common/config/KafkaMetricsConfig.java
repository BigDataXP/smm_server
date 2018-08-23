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

package com.hortonworks.smm.kafka.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.smm.kafka.common.errors.SMMConfigurationException;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaMetricsConfig {

    @JsonProperty("metrics.fetcher.class")
    private String metricsFetcherClass;

    @JsonProperty("metrics.cache.refresh.interval.ms")
    private long metricsCacheRefreshIntervalMs;

    @JsonProperty("consumer.group.refresh.interval.ms")
    private long consumerGroupRefreshIntervalMs;

    @JsonProperty("inactive.producer.timeout.ms")
    private long inactiveProducerTimeoutMs;

    @JsonProperty("inactive.group.timeout.ms")
    private long inactiveGroupTimeoutMs;

    @JsonProperty("metrics.fetcher.threads")
    private int metricsFetcherThreads;

    @JsonProperty("properties")
    private Map<String, Object> config;

    private KafkaMetricsConfig() {
    }

    public KafkaMetricsConfig(String metricsFetcherClass,
                              long metricsCacheRefreshIntervalMs,
                              long consumerGroupRefreshIntervalMs,
                              long inactiveProducerTimeoutMs,
                              long inactiveGroupTimeoutMs,
                              int metricsFetcherThreads,
                              Map<String, Object> properties) {
        if (metricsFetcherClass == null || metricsFetcherClass.isEmpty())
            throw new SMMConfigurationException(this.getClass(), "metrics.fetcher.class");

        this.metricsFetcherClass = metricsFetcherClass;
        this.metricsCacheRefreshIntervalMs = metricsCacheRefreshIntervalMs;
        this.consumerGroupRefreshIntervalMs = consumerGroupRefreshIntervalMs;
        this.inactiveProducerTimeoutMs = inactiveProducerTimeoutMs;
        this.inactiveGroupTimeoutMs = inactiveGroupTimeoutMs;
        this.metricsFetcherThreads = metricsFetcherThreads;
        this.config = properties;
    }

    public String getMetricsFetcherClass() {
        return metricsFetcherClass;
    }

    public long getMetricsCacheRefreshIntervalMs() {
        return metricsCacheRefreshIntervalMs;
    }

    public long getConsumerGroupRefreshIntervalMs() {
        return consumerGroupRefreshIntervalMs;
    }

    public long getInactiveProducerTimeoutMs() {
        return inactiveProducerTimeoutMs;
    }

    public long getInactiveGroupTimeoutMs() {
        return inactiveGroupTimeoutMs;
    }

    public int getMetricsFetcherThreads() {
        return metricsFetcherThreads;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "KafkaMetricsConfig{" +
                "metricsFetcherClass='" + metricsFetcherClass + '\'' +
                ", metricsCacheRefreshIntervalMs=" + metricsCacheRefreshIntervalMs +
                ", consumerGroupRefreshIntervalMs=" + consumerGroupRefreshIntervalMs +
                ", inactiveProducerTimeoutMs=" + inactiveProducerTimeoutMs +
                ", inactiveGroupTimeoutMs=" + inactiveGroupTimeoutMs +
                ", metricsFetcherThreads=" + metricsFetcherThreads +
                ", config=" + config +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMetricsConfig that = (KafkaMetricsConfig) o;
        return metricsCacheRefreshIntervalMs == that.metricsCacheRefreshIntervalMs &&
                consumerGroupRefreshIntervalMs == that.consumerGroupRefreshIntervalMs &&
                inactiveProducerTimeoutMs == that.inactiveProducerTimeoutMs &&
                inactiveGroupTimeoutMs == that.inactiveGroupTimeoutMs &&
                metricsFetcherThreads == that.metricsFetcherThreads &&
                Objects.equals(metricsFetcherClass, that.metricsFetcherClass) &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricsFetcherClass, metricsCacheRefreshIntervalMs, consumerGroupRefreshIntervalMs,
                inactiveProducerTimeoutMs, inactiveGroupTimeoutMs, metricsFetcherThreads, config);
    }
}
