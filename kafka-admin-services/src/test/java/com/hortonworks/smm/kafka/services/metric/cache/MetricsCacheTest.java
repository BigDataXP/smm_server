package com.hortonworks.smm.kafka.services.metric.cache;

import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricDescriptorSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.CONSUMER_GROUP_TAG;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.PRODUCER_TP_TAG;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.TOPIC_PARTITION_TAG;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.TOPIC_TAG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
public class MetricsCacheTest {

    private MetricsCache metricsCache;
    private MetricsFetcher metricsFetcher;

    @Before
    public void setUp() {
        metricsFetcher = mock(MetricsFetcher.class);
        when(metricsFetcher.getMetricDescriptorSupplier()).thenReturn(new AMSMetricDescriptorSupplier());
        metricsCache = new MetricsCache(metricsFetcher, null, null);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testFindAllMatchingDescriptors() {
        MetricDescriptorSupplier supplier = metricsFetcher.getMetricDescriptorSupplier();

        MetricDescriptor expectedTopicDescriptor = supplier.topicMessagesInCount(
                getQueryTags(TOPIC_TAG, "-"));
        List<MetricDescriptor> expectedTopicDescriptors = descriptorDataProvider(expectedTopicDescriptor);

        MetricDescriptor expectedPartitionDescriptor = supplier.topicPartitionMessagesInCount(
                getQueryTags(TOPIC_PARTITION_TAG, "-"));
        List<MetricDescriptor> expectedPartitionDescriptors = descriptorDataProvider(expectedPartitionDescriptor);

        MetricDescriptor expectedProducerDescriptor = supplier.producerMessagesInCount(
                getQueryTags(PRODUCER_TP_TAG, "-"));
        List<MetricDescriptor> expectedProducerDescriptors = descriptorDataProvider(expectedProducerDescriptor);

        MetricDescriptor expectedPartitionLagDescriptor = supplier.partitionLag (
                getQueryTags(CONSUMER_GROUP_TAG, "-"));
        List<MetricDescriptor> expectedPartitionLagDescriptors = descriptorDataProvider(expectedPartitionLagDescriptor);

        MetricDescriptor expectedPartitionLagRateDescriptor = supplier.partitionLagRate(
                getQueryTags(CONSUMER_GROUP_TAG, "-")
        );
        List<MetricDescriptor> expectedPartitionLagRateDescriptors = descriptorDataProvider(expectedPartitionLagRateDescriptor);

        Map<MetricsCache.MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> metricDescriptorWildCardResolutionMap = new HashMap<>();
        metricDescriptorWildCardResolutionMap.put(new MetricsCache.MetricDescriptorWildCardResolutionKey(
                        expectedTopicDescriptor.metricName(),
                        expectedTopicDescriptor.aggrFunction(),
                        expectedTopicDescriptor.postProcessFunction()),
                expectedTopicDescriptors);
        metricDescriptorWildCardResolutionMap.put(new MetricsCache.MetricDescriptorWildCardResolutionKey(
                        expectedPartitionDescriptor.metricName(),
                        expectedPartitionDescriptor.aggrFunction(),
                        expectedPartitionDescriptor.postProcessFunction()),
                expectedPartitionDescriptors);
        metricDescriptorWildCardResolutionMap.put(new MetricsCache.MetricDescriptorWildCardResolutionKey(
                        expectedProducerDescriptor.metricName(),
                        expectedProducerDescriptor.aggrFunction(),
                        expectedProducerDescriptor.postProcessFunction()),
                expectedProducerDescriptors);
        metricDescriptorWildCardResolutionMap.put(new MetricsCache.MetricDescriptorWildCardResolutionKey(
                        expectedPartitionLagDescriptor.metricName(),
                        expectedPartitionLagDescriptor.aggrFunction(),
                        expectedPartitionLagDescriptor.postProcessFunction()),
                expectedPartitionLagDescriptors);
        metricDescriptorWildCardResolutionMap.put(new MetricsCache.MetricDescriptorWildCardResolutionKey(
                        expectedPartitionLagRateDescriptor.metricName(),
                        expectedPartitionLagRateDescriptor.aggrFunction(),
                        expectedPartitionLagRateDescriptor.postProcessFunction()),
                expectedPartitionLagRateDescriptors);

        MetricDescriptor topicDescriptor = supplier.topicMessagesInCount(
                getQueryTags(TOPIC_TAG, MetricsService.WILD_CARD));
        checkEquals(metricDescriptorWildCardResolutionMap, expectedTopicDescriptors, topicDescriptor);

        MetricDescriptor partitionDescriptor = supplier.topicPartitionMessagesInCount(
                getQueryTags(TOPIC_PARTITION_TAG, MetricsService.WILD_CARD));
        checkEquals(metricDescriptorWildCardResolutionMap, expectedPartitionDescriptors, partitionDescriptor);

        MetricDescriptor producerDescriptor = supplier.producerMessagesInCount(
                getQueryTags(PRODUCER_TP_TAG, MetricsService.WILD_CARD));
        checkEquals(metricDescriptorWildCardResolutionMap, expectedProducerDescriptors, producerDescriptor);

        MetricDescriptor partitionLagDescriptor = supplier.partitionLag(
                getQueryTags(CONSUMER_GROUP_TAG, MetricsService.WILD_CARD));
        checkEquals(metricDescriptorWildCardResolutionMap, expectedPartitionLagDescriptors, partitionLagDescriptor);

        MetricDescriptor partitionLagRateDescriptor = supplier.partitionLagRate(
                getQueryTags(CONSUMER_GROUP_TAG, MetricsService.WILD_CARD));
        checkEquals(metricDescriptorWildCardResolutionMap, expectedPartitionLagRateDescriptors, partitionLagRateDescriptor);
    }

    @Test
    public void testMetricValueIntegrity() {
        long oneDayMs = TimeUnit.DAYS.toMillis(1);
        long startTimeMs = System.currentTimeMillis();
        Map<Long, Double> expectedMetrics = new LinkedHashMap<>();
        for (int i=0; i<50; i++) {
            long timestamp = startTimeMs + (i * oneDayMs);
            expectedMetrics.put(timestamp, 0.0);
        }

        MetricsCache.MetricValue metricValue = new MetricsCache.MetricValue(expectedMetrics);
        Assertions.assertEquals(expectedMetrics, metricValue.values());
    }

    private Map<String, String> getQueryTags(List<String> tags, String value) {
        Map<String, String> queryTags = new HashMap<>();
        for (String tag : tags) {
            queryTags.put(tag, "-".equals(value) ? randomAlphaNumeric() : value);
        }
        return queryTags;
    }

    private void checkEquals(Map<MetricsCache.MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> allDescriptors,
                             List<MetricDescriptor> expectedDescriptors,
                             MetricDescriptor descriptor) {
        List<MetricDescriptor> actualDescriptors = metricsCache.findAllMatchingDescriptors(allDescriptors, descriptor);
        Assertions.assertEquals(expectedDescriptors.size(), actualDescriptors.size());
        Assertions.assertTrue(actualDescriptors.containsAll(expectedDescriptors));
    }

    private List<MetricDescriptor> descriptorDataProvider(MetricDescriptor descriptor) {
        List<MetricDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Map<String, String> queryTags = new HashMap<>(descriptor.queryTags());
            queryTags.put(AbstractMetricDescriptorSupplier.TOPIC, "MyTopic-"+i);
            descriptors.add(MetricDescriptor.newBuilder()
                                .withAggregationFunction(descriptor.aggrFunction())
                                .withPostProcessFunction(descriptor.postProcessFunction())
                                .withQueryTags(queryTags)
                                .build(descriptor.metricName()));
        }
        return descriptors;
    }

    private String randomAlphaNumeric() {
        int count = 10;
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int) (Math.random() * alphaNumericString.length());
            builder.append(alphaNumericString.charAt(character));
        }
        return builder.toString();
    }
}