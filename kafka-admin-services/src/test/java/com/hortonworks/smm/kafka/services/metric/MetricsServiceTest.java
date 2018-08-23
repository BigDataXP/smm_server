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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.cache.MetricsCache;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.hortonworks.smm.kafka.services.metric.MetricsService.METRICS_IMPL_CLASSNAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MetricsServiceTest {
    private final String TEST_COLLECTOR_API_PATH = "/ws/v1/timeline/metrics";
    private final int PORT = ThreadLocalRandom.current().nextInt(4096, 65535);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.options().port(PORT), false);

    private MetricsService metricsService;
    private MetricDescriptorSupplier supplier;

    @Before
    public void setUp() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(MetricsService.METRICS_IMPL_CLASSNAME_KEY, "com.hortonworks.smm.kafka.services.metric.ams.AMSMetricsFetcher");
        configs.put("metrics.cache.refresh.interval.ms", "300000");
        configs.put("ams.timeline.metrics.port", PORT);

        MetricsFetcher metricsFetcher = createMetricsFetcher(configs);
        MetricsCache metricsCache = new MetricsCache(metricsFetcher, null, null);
        metricsService = new MetricsService(metricsFetcher, metricsCache);
        supplier = metricsFetcher.getMetricDescriptorSupplier();
    }

    @After
    public void tearDown() throws Exception {
        if (metricsService != null) {
            metricsService.close();
        }
    }

    @Test
    public void testBrokerBytesInCount() throws IOException {
        MetricDescriptor descriptor = supplier.brokerBytesInCount();
        Map<String, Double> generatedTimestampValues = generateTimestampToValueMap(100);
        Map<MetricDescriptor, Map<Long, Double>> expected = Collections.singletonMap(descriptor,
                getTimestampToValueMapWithKeyAsLong(generatedTimestampValues));

        String metricName = AMSMetricsFetcher.getAMSMetricName(descriptor);
        stubMetricUrl(metricName, generatedTimestampValues);
        BrokerNode node = BrokerNode.from(Node.noNode());
        Map<MetricDescriptor, Map<Long, Double>> actual = metricsService.getBrokerBytesInCount(node);
        assertEquals(expected, actual);
    }

    @Test
    public void testClusterBytesInCount() throws IOException {
        MetricDescriptor descriptor = supplier.clusterBytesInCount();
        Map<String, Double> generatedTimestampValues = generateTimestampToValueMap(100);
        Map<MetricDescriptor, Map<Long, Double>> expected = Collections.singletonMap(descriptor,
                getTimestampToValueMapWithKeyAsLong(generatedTimestampValues));

        String metricName = AMSMetricsFetcher.getAMSMetricName(descriptor);
        stubMetricUrl(metricName, generatedTimestampValues);
        Map<MetricDescriptor, Map<Long, Double>> actual = metricsService.getClusterBytesInCount();
        assertEquals(expected, actual);
    }

    @Test
    public void testAllProducerInMessagesCountWithNoMetrics() throws IOException {
        String metricName = AMSMetricsFetcher.getAMSMetricName(supplier.producerMessagesInCount());
        stubMetricUrl(metricName, null);

        Map<MetricDescriptor, Map<Long, Double>> result = metricsService.getAllProducerInMessagesCount(TimeSpan.EMPTY);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(Collections.emptyMap(), result.entrySet().iterator().next().getValue());
    }

    private void stubMetricUrl(String metricName, Map<String, Double> metricData) throws IOException {
        stubLiveNodeUrl();

        Map<String, List<Map<String, ?>>> stubBodyMap = new HashMap<>();

        List<Map<String, ?>> metrics = new ArrayList<>();

        Map<String, Object> metric1 = new HashMap<>();
        metric1.put("metricname", metricName);
        metric1.put("metrics", metricData);
        metrics.add(metric1);

        stubBodyMap.put("metrics", metrics);

        ObjectMapper mapper = new ObjectMapper();
        String body = mapper.writeValueAsString(stubBodyMap);

        stubFor(get(urlPathMatching(TEST_COLLECTOR_API_PATH))
                .withHeader("Accept", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));
    }

    private void stubLiveNodeUrl() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            stubFor(get(urlPathMatching("/ws/v1/timeline/metrics/livenodes"))
                    .withHeader("Accept", equalTo("text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody(mapper.writeValueAsString(Collections.singletonList("localhost")))));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Double> generateTimestampToValueMap(int entries) {
        Map<String, Double> timestampToValue = new HashMap<>();
        for (int i = 0; i < entries; i++) {
            String timestamp = String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong()));
            double value = Math.abs(ThreadLocalRandom.current().nextDouble());
            timestampToValue.put(timestamp, value);
        }
        return timestampToValue;
    }

    private Map<Long, Double> getTimestampToValueMapWithKeyAsLong(Map<String, Double> timeStampToValue) {
        Map<Long, Double> timestampToValueMap = new HashMap<>();
        timeStampToValue.forEach((k, v) -> timestampToValueMap.put(Long.parseLong(k), v));
        return timestampToValueMap;
    }

    private MetricsFetcher createMetricsFetcher(Map<String, Object> conf) {
        String metricsClassName = (String) conf.get(METRICS_IMPL_CLASSNAME_KEY);
        try {
            Constructor<?> constructor = Class.forName(metricsClassName).getConstructor(TopicManagementService.class);
            MetricsFetcher metricsFetcher = (MetricsFetcher) constructor.newInstance((TopicManagementService) null);
            metricsFetcher.configure(conf);
            return metricsFetcher;
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | ClassNotFoundException |
                InvocationTargetException | ConfigException e) {
            throw new RuntimeException("Can't initialize metrics fetcher class: " + metricsClassName, e);
        }
    }
}