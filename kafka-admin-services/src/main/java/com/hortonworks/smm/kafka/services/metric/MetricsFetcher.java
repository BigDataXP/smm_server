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
package com.hortonworks.smm.kafka.services.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import org.apache.kafka.common.Configurable;

public interface MetricsFetcher extends Configurable, AutoCloseable {

    /**
     * Get Broker level metrics
     *
     * @param brokerNode        Broker Node
     * @param startTimeMs       beginning of the time period: timestamp (in milliseconds)
     * @param endTimeMs         end of the time period: timestamp (in milliseconds).
     * @param metricDescriptors metric descriptors to fetch the data
     * @return Map of data points which are paired to (timestamp, value). If any of the startTimeMs, endTimeMs is less than zero,
     *        then latest value will be returned.
     */
    Map<MetricDescriptor, Map<Long, Double>> getBrokerMetrics(BrokerNode brokerNode,
                                                              long startTimeMs,
                                                              long endTimeMs,
                                                              Collection<MetricDescriptor> metricDescriptors);

    /**
     * Get Cluster Level metrics
     * @param startTimeMs         beginning of the time period: timestamp (in milliseconds)
     * @param endTimeMs           end of the time period: timestamp (in milliseconds)
     * @param metricDescriptors  metric names to fetch the data
     * @return  Map of data points which are paired to (timestamp, value). If any of the startTimeMs,
     *          endTimeMs is less than zero, then latest value will be returned.
     */
    Map<MetricDescriptor, Map<Long, Double>> getClusterMetrics(long startTimeMs,
                                                               long endTimeMs,
                                                               Collection<MetricDescriptor> metricDescriptors);

    /**
     * Get Host level metrics for Broker
     *
     * @param brokerNode        Broker Node
     * @param startTimeMs       beginning of the time period: timestamp (in milliseconds)
     * @param endTimeMs         end of the time period: timestamp (in milliseconds).
     * @param metricDescriptors metric descriptors to fetch the data
     * @return Map of data points which are paired to (timestamp, value). If any of the startTimeMs, endTimeMs is less than zero,
     *        then latest value will be returned.
     */
    Map<MetricDescriptor, Map<Long, Double>> getHostMetrics(BrokerNode brokerNode,
                                                            long startTimeMs,
                                                            long endTimeMs,
                                                            Collection<MetricDescriptor> metricDescriptors);

    /**
     * Returns MetricDescriptorSupplier instance which supplies the fetcher metric descriptors.
     *
     * @return MetricDescriptorSupplier instance
     */
    MetricDescriptorSupplier getMetricDescriptorSupplier();


    /**
     * Emit metrics to metrics storage system.
     * @param metrics   Metrics to store
     * @return true if successful, false if any error while emitting metrics
     */
    boolean emitMetrics(Map<MetricDescriptor, Long> metrics);


    MetricsFetcher NO_OP = new MetricsFetcher() {
        @Override
        public Map<MetricDescriptor, Map<Long, Double>> getBrokerMetrics(BrokerNode brokerNode,
                                                                         long startTimeMs,
                                                                         long endTimeMs,
                                                                         Collection<MetricDescriptor> metricDescriptors) {
            return Collections.emptyMap();
        }

        @Override
        public Map<MetricDescriptor, Map<Long, Double>> getHostMetrics(BrokerNode brokerNode,
                                                                       long startTimeMs,
                                                                       long endTimeMs,
                                                                       Collection<MetricDescriptor> metricDescriptors) {
            return Collections.emptyMap();
        }

        @Override
        public Map<MetricDescriptor, Map<Long, Double>> getClusterMetrics(long startTimeMs,
                                                                          long endTimeMs,
                                                                          Collection<MetricDescriptor> metricDescriptors) {
            return Collections.emptyMap();
        }

        @Override
        public MetricDescriptorSupplier getMetricDescriptorSupplier() {
            return new DummyMetricDescriptorSupplier();
        }

        @Override
        public boolean emitMetrics(Map<MetricDescriptor, Long> metrics) {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    };
}
