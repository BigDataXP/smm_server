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
package com.hortonworks.smm.kafka.services.clients;

import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.security.auth.SMMSecurityContext;
import com.hortonworks.smm.kafka.services.security.impl.NoopAuthorizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProducerMetricsTest {

    private MetricsService metricsService;
    private MetricDescriptorSupplier metricDescriptorSupplier;
    private TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.LAST_SIX_HOURS);
    private int totalProducers = 50;
    private int activeProducers = 10;

    @BeforeEach
    public void setUp() {
        metricsService = mock(MetricsService.class);
        metricDescriptorSupplier = new AMSMetricDescriptorSupplier();
    }

    @Test
    public void testProducerStateTagCases() {
        mockProducerMetrics();
        Collection<ProducerMetrics> producerMetrics = ProducerMetrics.from(metricsService, 1800_000L, ClientState.all,
                timeSpan, new NoopAuthorizer(), getSecurityContext());

        assertEquals(totalProducers, producerMetrics.size());
        assertEquals(activeProducers, producerMetrics.stream().filter(pm -> pm.active()).count());
        assertEquals(totalProducers-activeProducers, producerMetrics.stream().filter(pm -> !pm.active()).count());
        assertEquals(Optional.empty(),
                producerMetrics.stream()
                        .filter(pm -> pm.active())
                        .filter(pm -> Integer.parseInt(pm.clientId().split("-")[1]) >= activeProducers)
                        .findAny());
    }

    private SecurityContext getSecurityContext() {
        return new SMMSecurityContext(new Principal() {
            @Override
            public String getName() {
                return "test";
            }
        }, "http");
    }

    private void mockProducerMetrics() {
        when(metricsService.getAllProducerInMessagesCount(eq(timeSpan)))
                .thenAnswer(invocation -> getProducerMetrics(totalProducers));
        when(metricsService.getAllProducerInMessagesCount(AdditionalMatchers.not(eq(timeSpan))))
                .thenAnswer(invocation -> getProducerMetrics(activeProducers));
    }

    private Map<MetricDescriptor, Map<Long, Double>> getProducerMetrics(int nClients) {
        Map<MetricDescriptor, Map<Long, Double>> producerMetrics = new HashMap<>();
        for (int i=0; i<nClients; i++) {
            Map<String, String> queryTags = new HashMap<>();
            queryTags.put(AbstractMetricDescriptorSupplier.CLIENT_ID, "clientId-" + i);
            queryTags.put(AbstractMetricDescriptorSupplier.TOPIC, "topic-" + i);
            queryTags.put(AbstractMetricDescriptorSupplier.PARTITION_NUMBER, Integer.toString(i));
            producerMetrics.put(metricDescriptorSupplier.producerMessagesInCount(queryTags), getMetrics());
        }
        return producerMetrics;
    }

    private Map<Long, Double> getMetrics() {
        Map<Long, Double> metrics = new HashMap<>();
        metrics.put(2000L, 200D);
        metrics.put(4000L, 300D);
        metrics.put(6000L, 500D);
        metrics.put(10000L, 999D);
        return metrics;
    }

}