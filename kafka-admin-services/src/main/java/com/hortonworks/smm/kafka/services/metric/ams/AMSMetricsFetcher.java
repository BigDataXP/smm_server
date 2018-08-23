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

import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.AggregateFunction;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricName;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.PostProcessFunction;
import com.hortonworks.smm.kafka.utils.JsonUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.sink.timeline.AbstractTimelineMetricsSink;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetrics;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AMSMetricsFetcher extends AbstractTimelineMetricsSink implements MetricsFetcher {
    private static final Logger log = LoggerFactory.getLogger(AMSMetricsFetcher.class);

    // Default AMS appid for kafka broker
    private static final String DEFAULT_APP_ID = "kafka_broker";

    //Metrics constants
    private static final String APP_ID = "ams.kafka.appid";
    private static final String HOST_METRICS_APP_ID = "HOST";
    private static final String TIMELINE_METRICS_KAFKA_PREFIX = "ams.timeline.metrics.";
    private static final String TIMELINE_HOSTS_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + "hosts";
    private static final String TIMELINE_PORT_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + "port";
    private static final String TIMELINE_PROTOCOL_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + "protocol";
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_PATH_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + SSL_KEYSTORE_PATH_PROPERTY;
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_TYPE_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + SSL_KEYSTORE_TYPE_PROPERTY;
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_PASSWORD_PROPERTY = TIMELINE_METRICS_KAFKA_PREFIX + SSL_KEYSTORE_PASSWORD_PROPERTY;
    private static final String TIMELINE_DEFAULT_HOST = "localhost";
    private static final String TIMELINE_DEFAULT_PORT = "6188";
    private static final String TIMELINE_DEFAULT_PROTOCOL = "http";

    private String hostname;
    private String metricCollectorPort = TIMELINE_DEFAULT_PORT;
    private Collection<String> collectorHosts;
    private String metricCollectorProtocol = TIMELINE_DEFAULT_PROTOCOL;
    private int timeoutSeconds = 10;
    private String zookeeperQuorum = null;

    private static final long ONE_DAY_MS = TimeUnit.DAYS.toMillis(1);
    private static final long SEVEN_DAYS_MS = TimeUnit.DAYS.toMillis(7);

    private Client client;
    private String appId;

    public enum Precision {
        SECONDS, MINUTES, HOURS, DAYS
    }

    public AMSMetricsFetcher(TopicManagementService topicManagementService) {
    }

    @Override
    public void configure(Map<String, ?> conf) {
        if (conf != null) {
            appId = (String) conf.get(APP_ID);
            if (appId == null) {
                appId = DEFAULT_APP_ID;
            }
        }
        client = ClientBuilder.newClient(new ClientConfig());
        initMetrics(conf);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            log.warn("Error occurred while closing client", e);
        }
    }

    @Override
    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMetrics(BrokerNode brokerNode,
                                                                     long startTimeMs,
                                                                     long endTimeMs,
                                                                     Collection<MetricDescriptor> metricDescriptors) {
        return getMetrics(brokerNode.host(), startTimeMs, endTimeMs, metricDescriptors);
    }

    @Override
    public Map<MetricDescriptor, Map<Long, Double>> getHostMetrics(BrokerNode brokerNode,
                                                                     long startTimeMs,
                                                                     long endTimeMs,
                                                                     Collection<MetricDescriptor> metricDescriptors) {
        return getHostMetrics(brokerNode.host(), startTimeMs, endTimeMs, metricDescriptors);
    }

    @Override
    public Map<MetricDescriptor, Map<Long, Double>> getClusterMetrics(long startTimeMs,
                                                                      long endTimeMs,
                                                                      Collection<MetricDescriptor> metricDescriptors) {
        return getMetrics("", startTimeMs, endTimeMs, metricDescriptors);
    }

    @Override
    public MetricDescriptorSupplier getMetricDescriptorSupplier() {
        return new AMSMetricDescriptorSupplier();
    }

    private Map<MetricDescriptor, Map<Long, Double>> getMetrics(String hostName,
                                                                long startTime,
                                                                long endTime,
                                                                Collection<MetricDescriptor> metricDescriptors) {
        return getMetrics(hostName, startTime, endTime, metricDescriptors, appId);
    }

    private Map<MetricDescriptor, Map<Long, Double>> getHostMetrics(String hostName,
                                                                long startTime,
                                                                long endTime,
                                                                Collection<MetricDescriptor> metricDescriptors) {
        return getMetrics(hostName, startTime, endTime, metricDescriptors, HOST_METRICS_APP_ID);
    }

    @SuppressWarnings("unchecked")
    private Map<MetricDescriptor, Map<Long, Double>> getMetrics(String hostName,
                                                                long startTime,
                                                                long endTime,
                                                                Collection<MetricDescriptor> metricDescriptors,
                                                                String appId) {
        long beginTimeMs = System.currentTimeMillis();
        URI targetUri = buildURI(hostName, startTime, endTime, metricDescriptors, appId);
        log.trace("Calling URI : {}", targetUri.toString());

        Map<String, ?> responseMap = JsonUtils.getEntity(client.target(targetUri), Map.class);
        List<Map<String, ?>> metrics = (List<Map<String, ?>>) responseMap.get("metrics");
        Map<MetricDescriptor, Map<Long, Double>> targetResult = new HashMap<>();

        for (Map<String, ?> metric : metrics) {
            String retrievedMetricName = (String) metric.get("metricname");
            Map<String, Number> retrievedPoints = (Map<String, Number>) metric.get("metrics");

            Map<Long, Double> pointsForOutput;
            if (retrievedPoints == null || retrievedPoints.isEmpty()) {
                pointsForOutput = Collections.emptyMap();
            } else {
                pointsForOutput = new HashMap<>(retrievedPoints.size());
                for (Map.Entry<String, Number> timestampToValue : retrievedPoints.entrySet()) {
                    pointsForOutput.put(Long.valueOf(timestampToValue.getKey()), timestampToValue.getValue().doubleValue());
                }
            }
            targetResult.put(getMetricDescriptorFromAMSName(retrievedMetricName), pointsForOutput);
        }
        log.debug("URI : {}, receivedMetricPoints : {}, time taken : {} ms", targetUri.toString(), targetResult.size(),
                (System.currentTimeMillis() - beginTimeMs));
        log.trace("Received metrics : {}", targetResult);
        return targetResult;
    }

    private URI buildURI(String hostName,
                         long startTime,
                         long endTime,
                         Collection<MetricDescriptor> metricDescriptors,
                         String appId) {
        String metricNames = StringUtils.join(metricDescriptors.stream().map(x -> getAMSMetricName(x)).toArray(String[]::new), ",");

        JerseyUriBuilder uriBuilder = new JerseyUriBuilder();
        uriBuilder.uri(getCollectorAPIUri())
            .queryParam("appId", appId)
            .queryParam("metricNames", metricNames)
            .queryParam("precision", getPrecision(startTime, endTime).name());

        if (startTime > 0 && endTime > 0) {
            uriBuilder.queryParam("startTime", String.valueOf(startTime));
            uriBuilder.queryParam("endTime", String.valueOf(endTime));
        }

        if (hostName != null && !hostName.isEmpty()) {
            uriBuilder.queryParam("hostname", hostName);
        }

        return uriBuilder.build();
    }

    private String getCollectorAPIUri() {
        String collectorHost = getCurrentCollectorHost();
        return getCollectorUri(collectorHost);
    }

    public static String getAMSMetricName(MetricDescriptor metricDescriptor) {

        MetricName metricName = metricDescriptor.metricName();
        Map<String, String> queryTags = metricDescriptor.queryTags();
        AggregateFunction aggrFunction = metricDescriptor.aggrFunction();
        PostProcessFunction postProcessFunction = metricDescriptor.postProcessFunction();

        if (!queryTags.keySet().containsAll(metricName.getTags())) {
            throw new IllegalArgumentException("metric tags doesn't match with supplied query tags for : " + metricDescriptor);
        }

        StringBuilder amsName = new StringBuilder();
        amsName.append(metricName.getGroupName());
        append(amsName, metricName.getTypeName());
        append(amsName, metricName.getName());

        for (String tag : metricName.getTags()) {
            append(amsName, tag);
            append(amsName, queryTags.get(tag));
        }
        append(amsName, metricName.getAttribute());

        if (aggrFunction != null) {
            amsName.append("._");
            amsName.append(aggrFunction.name().toLowerCase());
        }

        if (postProcessFunction != null) {
            amsName.append("._");
            amsName.append(postProcessFunction.name().toLowerCase());
        }

        log.debug("metric name: {}", amsName);

        return amsName.toString();
    }

    private static StringBuilder append(StringBuilder builder, String toAppend) {
        if (toAppend != null && !toAppend.isEmpty()) {
            if (builder.length() > 0) {
                builder.append(".");
            }
            builder.append(toAppend);
        }
        return builder;
    }

    MetricDescriptor getMetricDescriptorFromAMSName(String amsName) {
        String[] chunks = amsName.split("\\.");
        StringBuilder groupNameBuilder = new StringBuilder();
        int groupIndex = -1;
        for (String chunk : chunks) {
            if (Character.isLowerCase(chunk.codePointAt(0))) {
                groupNameBuilder.append(chunk).append(".");
                groupIndex++;
            } else {
                break;
            }
        }

        MetricDescriptor.MetricDescriptorBuilder descriptorBuilder = MetricDescriptor.newBuilder();

        //TODO: This is hackish. Done specifically for host metrics
        if (chunks.length == 1) {
            MetricName metricName =   MetricName.newBuilder("").
                    name(chunks[0]).
                    typeName("").
                    attribute("").
                    tags(Collections.emptyList()).
                    build();
            return descriptorBuilder.build(metricName);
        }

        String groupName = groupNameBuilder.substring(0, groupNameBuilder.length() - 1);
        MetricName.Builder metricNameBuilder = MetricName.newBuilder(groupName);

        if (groupIndex + 2 <= chunks.length - 1) {
            int typeIndex = groupIndex + 1;
            int nameIndex = typeIndex + 1;

            metricNameBuilder.typeName(chunks[typeIndex]);
            metricNameBuilder.name(chunks[nameIndex]);

            int y = chunks.length - 1;
            for (; y > nameIndex; y--) {
                if (chunks[y].startsWith("_") && !chunks[y].startsWith("__")) {
                    String funcVal = chunks[y].split("_")[1].toUpperCase();
                    switch (funcVal) {
                        case "RATE":
                        case "DIFF":
                            descriptorBuilder.withPostProcessFunction(PostProcessFunction.valueOf(funcVal));
                            break;
                        default:
                            descriptorBuilder.withAggregationFunction(AggregateFunction.valueOf(funcVal));
                    }
                } else if (AbstractMetricDescriptorSupplier.ATTRIBUTES.contains(chunks[y])) {
                    metricNameBuilder.attribute(chunks[y]);
                } else {
                    break;
                }
            }

            if ((y - nameIndex) % 2 == 0) {
                LinkedHashMap<String, String> queryTags = new LinkedHashMap<>();
                for (int k = nameIndex + 1; k <= y; k = k + 2) { // key-value pairs
                    queryTags.put(chunks[k], chunks[k + 1]);
                }
                metricNameBuilder.tags(new ArrayList<>(queryTags.keySet()));
                descriptorBuilder.withQueryTags(queryTags);
            }
        }
        return descriptorBuilder.build(metricNameBuilder.build());
    }

    public static Precision getPrecision(long from, long to) {
        long timeDiff = to - from;

        if (timeDiff <= ONE_DAY_MS) {
            // up to one day set the precision to minute
            return Precision.MINUTES;
        } else if (timeDiff <= SEVEN_DAYS_MS) {
            // up to seven days set precision to hours
            return Precision.HOURS;
        }

        //above seven days set precision to days
        return Precision.DAYS;
    }


    @Override
    public boolean emitMetrics(Map<MetricDescriptor, Long> metricDescriptors) {
        List<TimelineMetric> metricsList = new ArrayList<>();

        long currentTimeMillis = System.currentTimeMillis();
        try {
            metricDescriptors.forEach((metricDescriptor, metricValue) -> {
                String metricName = getAMSMetricName(metricDescriptor);
                TimelineMetric metric = createTimelineMetric(currentTimeMillis, appId, metricName, metricValue);
                metricsList.add(metric);
            });
        } catch (Throwable t) {
            log.error("Exception while processing Kafka metric", t);
        }

        log.debug("Metrics List size: " + metricsList.size());
        log.debug("Metrics Set size: " + metricDescriptors.size());

        if (!metricsList.isEmpty()) {
            TimelineMetrics timelineMetrics = new TimelineMetrics();
            timelineMetrics.setMetrics(metricsList);
            try {
                return emitMetrics(timelineMetrics);
            } catch (Throwable t) {
                log.error("Exception emitting metrics", t);
                return false;
            }
        }
        return true;
    }

    private TimelineMetric createTimelineMetric(long currentTimeMillis,
                                                String component,
                                                String metricName,
                                                Number metricValue) {
        log.debug("Creating timeline metricName: {}, metricValue: {}, time: {}, app_id: {}", metricName, metricValue, currentTimeMillis, component);
        TimelineMetric timelineMetric = new TimelineMetric();
        timelineMetric.setMetricName(metricName);
        timelineMetric.setHostName(hostname);
        timelineMetric.setAppId(component);
        timelineMetric.setStartTime(currentTimeMillis);
        timelineMetric.setType(ClassUtils.getShortCanonicalName(metricValue, "Number"));
        timelineMetric.getMetricValues().put(currentTimeMillis, metricValue.doubleValue());
        return timelineMetric;
    }

    private void initMetrics(Map<String, ?> conf) {
        log.info("Initializing AMS Timeline Metrics Sink");
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            //If not FQDN , call  DNS
            if ((hostname == null) || (!hostname.contains("."))) {
                hostname = InetAddress.getLocalHost().getCanonicalHostName();
            }
        } catch (UnknownHostException e) {
            log.error("Could not identify hostname.");
            throw new RuntimeException("Could not identify hostname.", e);
        }
        // Initialize the collector write strategy
        super.init();

        zookeeperQuorum = conf.containsKey(COLLECTOR_ZOOKEEPER_QUORUM) ?
            (String) conf.get(COLLECTOR_ZOOKEEPER_QUORUM) : (String) conf.get("zookeeper.connect");

        if (conf.containsKey(TIMELINE_PORT_PROPERTY)) {
            metricCollectorPort = String.valueOf(conf.get(TIMELINE_PORT_PROPERTY));
        }

        String hosts = conf.containsKey(TIMELINE_HOSTS_PROPERTY) ?
            (String) conf.get(TIMELINE_HOSTS_PROPERTY) : TIMELINE_DEFAULT_HOST;
        collectorHosts = parseHostsStringIntoCollection(hosts);

        if (conf.containsKey(TIMELINE_PROTOCOL_PROPERTY)) {
            metricCollectorProtocol = (String) conf.get(TIMELINE_PROTOCOL_PROPERTY);
        }

        if (metricCollectorProtocol.contains("https")) {
            String trustStorePath = ((String) conf.get(TIMELINE_METRICS_SSL_KEYSTORE_PATH_PROPERTY)).trim();
            String trustStoreType = ((String) conf.get(TIMELINE_METRICS_SSL_KEYSTORE_TYPE_PROPERTY)).trim();
            String trustStorePwd = ((String) conf.get(TIMELINE_METRICS_SSL_KEYSTORE_PASSWORD_PROPERTY)).trim();
            loadTruststore(trustStorePath, trustStoreType, trustStorePwd);
        }

        log.info("hostName : {}", hostname);
        log.info("zookeeperQuorum : {}", zookeeperQuorum);
        log.info("metricCollectorPort : {}", metricCollectorPort);
        log.info("collectorHosts : {}", collectorHosts);
        log.info("metricCollectorProtocol : {}", metricCollectorProtocol);
    }

    @Override
    protected String getCollectorUri(String host) {
        return constructTimelineMetricUri(metricCollectorProtocol, host, metricCollectorPort);
    }

    @Override
    protected String getCollectorProtocol() {
        return metricCollectorProtocol;
    }

    @Override
    protected String getCollectorPort() {
        return metricCollectorPort;
    }

    @Override
    protected int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    @Override
    protected String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    @Override
    protected Collection<String> getConfiguredCollectorHosts() {
        return collectorHosts;
    }

    @Override
    protected String getHostname() {
        return hostname;
    }
}
