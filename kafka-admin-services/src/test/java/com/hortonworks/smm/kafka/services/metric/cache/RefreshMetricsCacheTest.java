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

package com.hortonworks.smm.kafka.services.metric.cache;

import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.MockMetricService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricDescriptorSupplier;
import org.apache.kafka.common.Node;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RefreshMetricsCacheTest {
    private MetricsFetcher metricsFetcher;
    private BrokerManagementService brokerMgmtService;

    private BrokerNode brokerNode;
    private MetricDescriptorSupplier metricDescriptorSupplier;
    private KafkaMetricsConfig metricsConfig;

    @Before
    public void setUp() {
        brokerNode = BrokerNode.from(new Node(1001, "ctr-e138-1518143905142-163245-01-000007.hwx.site", 6667));

        brokerMgmtService = mock(BrokerManagementService.class);
        when(brokerMgmtService.allBrokers()).thenReturn(Collections.singletonList(brokerNode));

        metricsFetcher = mock(MetricsFetcher.class);
        metricDescriptorSupplier = new AMSMetricDescriptorSupplier();
        when(metricsFetcher.getMetricDescriptorSupplier()).thenReturn(metricDescriptorSupplier);

        metricsConfig = new KafkaMetricsConfig(MockMetricService.class.getName(),
                                               100000L,
                                               3000L,
                                               1800_000L,
                                               1800_000L,
                                               20,
                                               Collections.emptyMap());
    }

    @Test
    public void testCacheRefresh() {
        mockBrokerMetrics();
        mockClusterMetrics();
        mockBrokerHostMetrics(getMetrics1(), getTotalMemory());
        MetricsCache metricsCache = new MetricsCache(metricsFetcher, metricsConfig, brokerMgmtService);

        assertBrokerMetric(metricsCache, metricDescriptorSupplier.brokerBytesInCount(), getMetrics1());
        assertBrokerMetric(metricsCache, metricDescriptorSupplier.brokerBytesInRate(), getMetrics2());

        assertBrokerMetric(metricsCache, metricDescriptorSupplier.cpuIdle(), getMetrics1());
        assertBrokerMetric(metricsCache, metricDescriptorSupplier.memFreePercent(), getMetrics2());

        assertClusterMetric(metricsCache, metricDescriptorSupplier.clusterBytesInCount(), getMetrics1());
        assertClusterMetric(metricsCache, metricDescriptorSupplier.clusterBytesInRate(), getMetrics2());

    }

    @Test
    public void testMemFreePercentEdgeCases() {
        mockBrokerHostMetrics(new HashMap<>(), getTotalMemory());
        MetricsCache metricsCache = new MetricsCache(metricsFetcher, metricsConfig, brokerMgmtService);
        assertBrokerMetric(metricsCache, metricDescriptorSupplier.memFreePercent(), new HashMap<>());

        mockBrokerHostMetrics(getMetrics1(), new HashMap<>());
        metricsCache = new MetricsCache(metricsFetcher, metricsConfig, brokerMgmtService);
        assertBrokerMetric(metricsCache, metricDescriptorSupplier.memFreePercent(), new HashMap<>());

    }

    @Test
    public void testFlushOnCacheRefresh() {
        MetricsCache metricsCache = new MetricsCache(metricsFetcher, metricsConfig, brokerMgmtService);
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.LAST_ONE_HOUR);
        when(metricsFetcher.getBrokerMetrics(eq(brokerNode), anyLong(), anyLong(),
                eq(Collections.singleton(metricDescriptorSupplier.brokerBytesInCount()))))
                .thenAnswer(new Answer<Map<MetricDescriptor, Map<Long, Double>>>() {
                    int counter = 0;

                    @Override
                    public Map<MetricDescriptor, Map<Long, Double>> answer(InvocationOnMock invocation) {
                        Map<MetricDescriptor, Map<Long, Double>> brokerMetrics = new HashMap<>();
                        long timeDiffMs = ((Long) invocation.getArguments()[2]) - ((Long) invocation.getArguments()[1]);
                        if (timeDiffMs == TimeUnit.HOURS.toMillis(1)) {
                            if (counter == 0) {
                                brokerMetrics.put(metricDescriptorSupplier.brokerBytesInCount(), getMetrics1());
                            } else if (counter == 1) {
                                brokerMetrics.put(metricDescriptorSupplier.brokerBytesInCount(), getMetrics2());
                            }
                            counter++;
                        }
                        return brokerMetrics;
                    }
                });

        metricsCache.refresh();
        Map<MetricDescriptor, Map<Long, Double>> brokerMetrics = metricsCache.getBrokerMetrics(brokerNode, timeSpan,
                metricDescriptorSupplier.brokerBytesInCount());
        assertEquals(Collections.singletonMap(metricDescriptorSupplier.brokerBytesInCount(), getMetrics1()),
                brokerMetrics);

        metricsCache.refresh();
        brokerMetrics = metricsCache.getBrokerMetrics(brokerNode, timeSpan, metricDescriptorSupplier.brokerBytesInCount());
        assertEquals(Collections.singletonMap(metricDescriptorSupplier.brokerBytesInCount(), getMetrics2()),
                brokerMetrics);

        metricsCache.refresh();
        brokerMetrics = metricsCache.getBrokerMetrics(brokerNode, timeSpan, metricDescriptorSupplier.brokerBytesInCount());
        assertTrue(brokerMetrics.isEmpty());
    }

    private void assertBrokerMetric(MetricsCache metricsCache, MetricDescriptor metricDescriptor, Map<Long, Double> expected) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.LAST_ONE_HOUR);
        Map<Long, Double> metric = metricsCache.getBrokerMetrics(brokerNode, timeSpan, metricDescriptor).
                get(metricDescriptor);
        assertEquals(expected, metric);
    }

    private void assertClusterMetric(MetricsCache metricsCache, MetricDescriptor metricDescriptor, Map<Long, Double> expected) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.LAST_ONE_HOUR);
        Map<Long, Double> metric = metricsCache.getClusterMetrics(timeSpan, metricDescriptor).
                get(metricDescriptor);
        assertEquals(expected, metric);
    }

    private void mockBrokerMetrics() {
        when(metricsFetcher.getBrokerMetrics(eq(brokerNode), anyLong(), anyLong(),
                eq(Collections.singleton(metricDescriptorSupplier.brokerBytesInCount())))).
                thenReturn(Collections.singletonMap(metricDescriptorSupplier.brokerBytesInCount(), getMetrics1()));

        when(metricsFetcher.getBrokerMetrics(eq(brokerNode), anyLong(), anyLong(),
                eq(Collections.singleton(metricDescriptorSupplier.brokerBytesInRate())))).
                thenReturn(Collections.singletonMap(metricDescriptorSupplier.brokerBytesInRate(), getMetrics2()));
    }


    // Every invocation should return a different object because multiple threads are modifying the response concurrently
    private void mockBrokerHostMetrics(Map<Long, Double> memFree, Map<Long, Double> memTotal) {
        when(metricsFetcher.getHostMetrics(eq(brokerNode), anyLong(), anyLong(), anyCollectionOf(MetricDescriptor.class))).
                thenAnswer(invocation -> getBrokerHostMetrics(memFree, memTotal));
    }

    private void mockClusterMetrics() {
        when(metricsFetcher.getClusterMetrics(anyLong(), anyLong(),
                eq(Collections.singleton(metricDescriptorSupplier.clusterBytesInCount()))))
                .thenReturn(Collections.singletonMap(metricDescriptorSupplier.clusterBytesInCount(), getMetrics1()));

        when(metricsFetcher.getClusterMetrics(anyLong(), anyLong(),
                eq(Collections.singleton(metricDescriptorSupplier.clusterBytesInRate()))))
                .thenReturn(Collections.singletonMap(metricDescriptorSupplier.clusterBytesInRate(), getMetrics2()));
    }

    private Map<MetricDescriptor, Map<Long, Double>> getBrokerHostMetrics(Map<Long, Double> memFree, Map<Long, Double> memTotal) {
        Map<MetricDescriptor, Map<Long, Double>> brokeHostMetrics = new HashMap<>();
        brokeHostMetrics.put(metricDescriptorSupplier.cpuIdle(), getMetrics1());
        brokeHostMetrics.put(metricDescriptorSupplier.memFree(), new HashMap<>(memFree));
        brokeHostMetrics.put(metricDescriptorSupplier.memTotal(), new HashMap<>(memTotal));
        return brokeHostMetrics;
    }

    private Map<Long, Double> getMetrics1() {
        Map<Long, Double> metrics = new HashMap<>();
        metrics.put(2000L, 100.0);
        metrics.put(4000L, 140.9);
        metrics.put(6000L, 103.2);
        metrics.put(9000L, 304.1);
        return metrics;
    }

    private Map<Long, Double> getMetrics2() {
        Map<Long, Double> metrics = new HashMap<>();
        metrics.put(2000L, 10.00);
        metrics.put(4000L, 14.09);
        metrics.put(6000L, 10.32);
        metrics.put(9000L, 30.41);
        return metrics;
    }

    private Map<Long, Double> getTotalMemory() {
        Map<Long, Double> metrics = new HashMap<>();
        metrics.put(2000L, 1000D);
        metrics.put(4000L, 1000D);
        metrics.put(6000L, 1000D);
        metrics.put(9000L, 1000D);
        return metrics;
    }
}
