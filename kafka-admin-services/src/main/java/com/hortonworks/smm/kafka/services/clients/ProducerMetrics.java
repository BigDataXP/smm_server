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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartition;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import javax.ws.rs.core.SecurityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProducerMetrics {

    @JsonProperty
    private String clientId;

    @JsonProperty
    private boolean active;

    @JsonProperty
    private Map<String, Map<Integer, Map<Long, Long>>> outMessagesCount;

    private ProducerMetrics() {
    }

    private ProducerMetrics(String clientId,
                            Map<TopicPartition, Map<Long, Long>> outMessagesCount) {
        this.clientId = clientId;
        this.outMessagesCount = TopicPartition.transformTopicPartitionWithMap(outMessagesCount);
    }

    public String clientId() {
        return clientId;
    }

    public boolean active() {
        return active;
    }

    public Map<String, Map<Integer, Map<Long, Long>>> outMessagesCount() {
        return outMessagesCount;
    }

    @Override
    public String toString() {
        return "ProducerMetrics{" +
                "clientId='" + clientId + '\'' +
                ", active='" + active + '\'' +
                ", outMessagesCount='" + outMessagesCount + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerMetrics that = (ProducerMetrics) o;
        return active == that.active &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(outMessagesCount, that.outMessagesCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, active, outMessagesCount);
    }

    public static Collection<ProducerMetrics> from(MetricsService metricsService,
                                                   Long inactiveProducerTimeoutMs,
                                                   ClientState state,
                                                   TimeSpan timeSpan,
                                                   SMMAuthorizer authorizer,
                                                   SecurityContext securityContext) {
        Collection<ProducerMetrics> allProducerMetrics = from(metricsService.getAllProducerInMessagesCount(timeSpan), authorizer, securityContext);
        tagActiveProducers(allProducerMetrics, null, metricsService, inactiveProducerTimeoutMs, timeSpan, authorizer, securityContext);

        if (state == ClientState.all) {
            return allProducerMetrics;
        } else {
            Collection<ProducerMetrics> producerMetricsList = new ArrayList<>();
            for (ProducerMetrics apm : allProducerMetrics) {
                if ((apm.active() && state == ClientState.active) ||
                        (!apm.active() && state == ClientState.inactive)) {
                    producerMetricsList.add(apm);
                }
            }
            return producerMetricsList;
        }
    }

    private static void tagActiveProducers(Collection<ProducerMetrics> producerMetrics,
                                           String clientId,
                                           MetricsService metricsService,
                                           Long inactiveProducerTimeoutMs,
                                           TimeSpan originalTimeSpan,
                                           SMMAuthorizer authorizer,
                                           SecurityContext securityContext) {
        // finding recent producer metrics to distinguish the active and passive producers
        TimeSpan activeTimeSpan = null;
        for (TimeSpan.TimePeriod timePeriod : TimeSpan.TimePeriod.values()) { // queries from cache if the range fits in
            if (timePeriod.millis() == inactiveProducerTimeoutMs) {
                activeTimeSpan = new TimeSpan(timePeriod);
                break;
            }
        }
        if (activeTimeSpan == null) { // queries from AMS
            long now = System.currentTimeMillis();
            activeTimeSpan = new TimeSpan(now-inactiveProducerTimeoutMs, now);
        }

        if (originalTimeSpan.startTimeMs() >= activeTimeSpan.startTimeMs()) { // all of them are active
            producerMetrics.forEach(pm -> pm.active = true);
        } else {
            Collection<ProducerMetrics> activeProducerMetrics = (clientId == null) ?
                from(metricsService.getAllProducerInMessagesCount(activeTimeSpan), authorizer, securityContext) :
                    from(metricsService.getProducerInMessagesCount(activeTimeSpan, clientId), authorizer, securityContext);

            List<String> activeProducers = new ArrayList<>();
            activeProducerMetrics.forEach(apm -> activeProducers.add(apm.clientId()));
            for (ProducerMetrics pm : producerMetrics) {
                if (activeProducers.contains(pm.clientId)) {
                    pm.active = true;
                }
            }
        }
    }

    private static Collection<ProducerMetrics> from(Map<MetricDescriptor, Map<Long, Double>> producersInMessageCountMap,
                                                    SMMAuthorizer authorizer,
                                                    SecurityContext securityContext) {
        Map<String, List<MetricDescriptor>> clientIdToDescriptors =
                groupDescriptorByClientId(producersInMessageCountMap);
        List<ProducerMetrics> allProducerMetrics = new ArrayList<>(clientIdToDescriptors.size());

        clientIdToDescriptors.forEach((clientId, descriptors) -> {
            Map<TopicPartition, Map<Long, Long>> partitionToInMessageCountMap = new HashMap<>();
            descriptors.forEach(descriptor -> {
                Map<String, String> tags = descriptor.queryTags();
                TopicPartition topicPartition =
                        new TopicPartition(tags.get("topic"), Integer.parseInt(tags.get("partition")));

                if (!SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicPartition.topic()))
                    return;

                Map<Long, Long> valueMap = new HashMap<>();
                producersInMessageCountMap.get(descriptor).forEach((k, v) -> valueMap.put(k, v.longValue()));
                partitionToInMessageCountMap.put(topicPartition, valueMap);
            });
            allProducerMetrics.add(new ProducerMetrics(clientId, partitionToInMessageCountMap));
        });
        return allProducerMetrics;
    }

    public static ProducerMetrics from(String clientId,
                                       MetricsService metricsService,
                                       Long inactiveProducerTimeoutMs,
                                       TimeSpan timeSpan,
                                       SMMAuthorizer authorizer, SecurityContext securityContext) {
        Map<MetricDescriptor, Map<Long, Double>> result = metricsService.getProducerInMessagesCount(timeSpan, clientId);
        ProducerMetrics producerMetrics = from(clientId, result, authorizer, securityContext);
        if (!result.isEmpty() && !result.entrySet().iterator().next().getValue().isEmpty()) {
            tagActiveProducers(Collections.singleton(producerMetrics), clientId, metricsService,
                    inactiveProducerTimeoutMs, timeSpan, authorizer, securityContext);
        }
        return producerMetrics;
    }

    private static ProducerMetrics from(String clientId,
                                       Map<MetricDescriptor, Map<Long, Double>> producersInMessageCountMap,
                                        SMMAuthorizer authorizer, SecurityContext securityContext) {
        Map<TopicPartition, Map<Long, Long>> partitionToInMessageCountMap = new HashMap<>();
        for (Map.Entry<MetricDescriptor, Map<Long, Double>> entry : producersInMessageCountMap.entrySet()) {
            MetricDescriptor descriptor = entry.getKey();
            Map<Long, Long> valueMap = new HashMap<>();
            entry.getValue().forEach((k, v) -> valueMap.put(k, v.longValue()));
            Map<String, String> tags = descriptor.queryTags();

            TopicPartition topicPartition =
                    new TopicPartition(tags.get("topic"), Integer.parseInt(tags.get("partition")));

            if (!SecurityUtil.authorizeTopicDescribe(authorizer, securityContext,topicPartition.topic()))
                continue;

            partitionToInMessageCountMap.put(topicPartition, valueMap);
        }
        return new ProducerMetrics(clientId, partitionToInMessageCountMap);
    }

    private static Map<String, List<MetricDescriptor>> groupDescriptorByClientId(
            Map<MetricDescriptor, Map<Long, Double>> producersInMessageCountMap) {
        Map<String, List<MetricDescriptor>> clientIdToDescriptors = new HashMap<>();
        producersInMessageCountMap.forEach(((descriptor, valueMap) -> {
            String clientId = descriptor.queryTags().get("clientId");
            clientIdToDescriptors.computeIfAbsent(clientId, x -> new ArrayList<>()).add(descriptor);
        }));
        return clientIdToDescriptors;
    }
}
