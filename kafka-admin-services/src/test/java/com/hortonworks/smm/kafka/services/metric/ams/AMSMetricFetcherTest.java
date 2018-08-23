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

package com.hortonworks.smm.kafka.services.metric.ams;

import java.util.Collections;

import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AMSMetricFetcherTest {

    private AMSMetricsFetcher metricsFetcher;
    private MetricDescriptorSupplier supplier;

    @Before
    public void setUp() {
        metricsFetcher = new AMSMetricsFetcher(null);
        supplier = new AMSMetricDescriptorSupplier();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testAMSMetricName() {

        // test simple metric name
        MetricDescriptor metricDescriptor = supplier.brokerBytesInCount();
        String amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.count", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // test metricDescriptor with aggregate function
        metricDescriptor = supplier.clusterBytesInAvg();
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.count._avg", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // test metricDescriptor with post processing function
        metricDescriptor = supplier.brokerBytesInDiff();
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.count._diff", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // test metricDescriptor with both aggregate and post processing function
        metricDescriptor = supplier.clusterBytesInDiff();
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.count._sum._diff", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // test metricDescriptor with query tags
        metricDescriptor = supplier.topicBytesInDiff(Collections.singletonMap("topic", "MY-TOPIC"));
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.topic.MY-TOPIC.count._sum._diff", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // verify wild cards in query tags
        metricDescriptor = supplier.topicBytesInDiff(Collections.singletonMap("topic", "%"));
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("kafka.server.BrokerTopicMetrics.BytesInPerSec.topic.%.count._sum._diff", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));

        // verify host metric names
        metricDescriptor = supplier.cpuIdle();
        amsName = metricsFetcher.getAMSMetricName(metricDescriptor);
        assertEquals("cpu_idle", amsName);
        Assert.assertEquals(metricDescriptor, metricsFetcher.getMetricDescriptorFromAMSName(amsName));
    }

}
