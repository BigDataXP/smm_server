package com.hortonworks.smm.kafka.services;

import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.ams.AMSMetricsFetcher;
import org.apache.kafka.common.Node;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sample code to test with AMS Collector
 */
public class AMSMetricsFetcherTest {
    private MetricsFetcher metricsFetcher;
    private MetricDescriptorSupplier supplier;
    private BrokerNode[] nodes;

    public AMSMetricsFetcherTest() {
        metricsFetcher = new AMSMetricsFetcher(null);
        supplier = new AMSMetricDescriptorSupplier();

        Map<String, String> conf = new HashMap<>();
        conf.put("ams.timeline.metrics.hosts", "172.27.19.10");
        conf.put("ams.timeline.metrics.port", "6188");

        metricsFetcher.configure(conf);

        nodes = new BrokerNode[] {
                BrokerNode.from(new Node(1001, "ctr-e138-1518143905142-163245-01-000007.hwx.site", 6667)),
                BrokerNode.from(new Node(1003, "ctr-e138-1518143905142-163245-01-000004.hwx.site", 6667)),
                BrokerNode.from(new Node(1002, "ctr-e138-1518143905142-163245-01-000009.hwx.site", 6667)),
        };
    }

    public static void main(String[] args) {
        AMSMetricsFetcherTest test = new AMSMetricsFetcherTest();

        //test fetch metrics from AMS
        test.getMetrics();

        //test emit metrics to AMS
        test.emitMetrics();

    }

    public void getMetrics() {
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));

        BrokerNode node = nodes[0];
        MetricDescriptor metricDescriptor = supplier.brokerMessagesInCount();

        Map<MetricDescriptor, Map<Long, Double>> metricValues =
                metricsFetcher.getBrokerMetrics(node,
                                                twoHoursAgo.toEpochMilli(),
                                                oneHourAgo.toEpochMilli(),
                                                Collections.singletonList(metricDescriptor));
        printMetrics(node, metricValues);

        //get current messages in count for a broker
        metricValues = metricsFetcher.getBrokerMetrics(node, -1, -1, Collections.singletonList(metricDescriptor));
        printMetrics(node, metricValues);

        //get rate for messages in count of a broker  for a given period
        metricDescriptor = supplier.brokerMessagesInRate();
        metricValues = metricsFetcher.getBrokerMetrics(node,
                                                       twoHoursAgo.toEpochMilli(),
                                                       oneHourAgo.toEpochMilli(),
                                                        Collections.singletonList(metricDescriptor));
        printMetrics(node, metricValues);

        //get messages in count for topic
        metricDescriptor = supplier.topicMessagesInCount(Collections.singletonMap("topic", "mytopic"));
        metricValues = metricsFetcher.getClusterMetrics(twoHoursAgo.toEpochMilli(),
                                                        oneHourAgo.toEpochMilli(),
                                                        Collections.singletonList(metricDescriptor));
        printMetrics(metricValues);


        //get messages in count for for all topics
        metricDescriptor = supplier.topicMessagesInCount(Collections.singletonMap("topic", "%"));
        metricValues = metricsFetcher.getClusterMetrics(twoHoursAgo.toEpochMilli(),
                                                        oneHourAgo.toEpochMilli(),
                                                        Collections.singletonList(metricDescriptor));
        printMetrics(metricValues);

        //get messages in count for for all topics
        Map<String, String> queryTags = new HashMap<>();
        queryTags.put("topic", "%");
        queryTags.put("partition", "%");
        metricDescriptor = supplier.topicPartitionMessagesInCount(queryTags);
        metricValues = metricsFetcher.getClusterMetrics(twoHoursAgo.toEpochMilli(),
                oneHourAgo.toEpochMilli(),
                Collections.singletonList(metricDescriptor));
        printMetrics(metricValues);
    }

    private static void printMetrics(Map<MetricDescriptor, Map<Long, Double>> metricValues) {
        printMetrics(null, metricValues);
    }

    private static void printMetrics(BrokerNode node, Map<MetricDescriptor, Map<Long, Double>> metricValues) {
        if (node != null) {
            System.out.println(node.toString());
        }

        metricValues.forEach((name, metricMap) -> {
            TreeMap<Long, Double> map = new TreeMap<>(metricMap);
            System.out.println(name + " contains " + map.values().size() + " values");
            map.forEach((timestamp, val) -> System.out.println("\t\t" + new Date(timestamp) + " -> " + val));
            System.out.println();
        });
    }


    private void emitMetrics() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(AbstractMetricDescriptorSupplier.CLIENT_ID, "clientId1");
        tags.put(AbstractMetricDescriptorSupplier.CONSUMER_GROUP, "grp1");
        tags.put(AbstractMetricDescriptorSupplier.TOPIC, "topic1");
        tags.put(AbstractMetricDescriptorSupplier.PARTITION_NUMBER, "0");

        Map<MetricDescriptor, Long> metrics = new HashMap<>();
        MetricDescriptor md1 = supplier.partitionLag(tags);
        metrics.put(md1, 10L);

        MetricDescriptor md2 = supplier.partitionCommittedOffset(tags);
        metrics.put(md2, 9L);

        metricsFetcher.emitMetrics(metrics);

    }
}
