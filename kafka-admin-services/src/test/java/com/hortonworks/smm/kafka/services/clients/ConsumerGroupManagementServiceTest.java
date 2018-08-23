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

import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerPartitionInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartition;
import com.hortonworks.smm.kafka.services.metric.MockMetricService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hortonworks.smm.kafka.services.clients.ConsumerGroupsService.COMMIT_OFFSETS_REFRESH_INTERVAL_MS;

/**
 *
 */
public class ConsumerGroupManagementServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupManagementServiceTest.class);

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    public static final long DEFAULT_WAIT_TIME_MS = 30_000L;
    public static final String CLIENT_ID_FORMAT = "%s-clientId-%s";

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testConsumerAPIsWithSinglePartitionAndSingleConsumer() throws Exception {
        String prefix = testName.getMethodName();
        String topic = prefix + "-topic";
        CLUSTER.createTopic(topic);

        //produce some data
        int noOfMsgs = 10;
        List<String> produceMessages =
                IntStream.range(0, noOfMsgs).boxed().map(x -> "message-" + x).collect(Collectors.toList());
        produceData(topic, produceMessages);

        //start a consumer group with single consumer instance
        String group = prefix + "-group";
        ConsumerGroupExecutor executor = null;
        ConsumerGroupManagementService consumerGroupManagementService = null;
        try {
            executor = new ConsumerGroupExecutor(1, 1, topic, group);
            consumerGroupManagementService = getConsumerGroupManagementService();
            waitTillStable(consumerGroupManagementService, group, 500L);

            executor.waitTillMsgCount(noOfMsgs, DEFAULT_WAIT_TIME_MS);

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();

            Assert.assertTrue(consumerGroupManagementService.consumerGroupNames().contains(group));

            ConsumerGroupInfo consumerGroupInfo = consumerGroupManagementService.consumerGroup(group);
            Integer partition = 0;
            TopicPartition tp = new TopicPartition(topic, partition);

            Assert.assertEquals(group, consumerGroupInfo.id());
            Assert.assertEquals("Stable", consumerGroupInfo.state());
            Assert.assertTrue(Collections.singletonList(tp).containsAll(topicPartitions(consumerGroupInfo)));

            verifyCommittedOffsets(consumerGroupInfo, 10L);
            verifyTotalLag(consumerGroupInfo, 0L);
            verifyUniqueClientIds(consumerGroupInfo, 1);

            //check consumer Info
            String clientId = fetchClientId(group, 0);
            ConsumerInfo consumerInfo = consumerGroupManagementService.consumerInfo(clientId);

            Assert.assertEquals(clientId, consumerInfo.clientId());
            ConsumerPartitionInfo consumerPartitionInfo = consumerInfo.consumerPartitionInfos().get(0);
            Assert.assertEquals(0L, consumerPartitionInfo.topicPartitionOffsets().get(topic).get(partition).lag()
                                                         .longValue());
            Assert.assertEquals(10L, consumerPartitionInfo.topicPartitionOffsets().get(topic).get(partition).offset()
                                                          .longValue());

            //stop the consumer group
            executor.close();

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();
            consumerGroupInfo = consumerGroupManagementService.consumerGroup(group);
            Assert.assertEquals(group, consumerGroupInfo.id());
            Assert.assertEquals("Empty", consumerGroupInfo.state());
        } finally {
            closeQuietly(executor, consumerGroupManagementService);
        }
    }

    private void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    private void waitTillStable(ConsumerGroupManagementService metricsService, String group, long sleepTime) {
        waitTillState(metricsService, group, sleepTime, "Stable");
    }

    private void waitTillEmpty(ConsumerGroupManagementService metricsService, String group, long sleepTime) {
        waitTillState(metricsService, group, sleepTime, "Empty");
    }

    private void waitTillState(ConsumerGroupManagementService metricsService, String group, long sleepTime, String state) {
        long startTime = System.currentTimeMillis();
        long waitTime = TimeUnit.SECONDS.toMillis(60);
        while (System.currentTimeMillis() < startTime + waitTime) {

            metricsService.waitTillRefreshConsumerGroupInfo();
            ConsumerGroupInfo consumerGroupInfo = metricsService.consumerGroup(group);

            if (Objects.nonNull(consumerGroupInfo) && state.equals(consumerGroupInfo.state())) {
                return;
            }

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
        }
        throw new IllegalStateException("Consumer Group : " + group + " is not [" + state + "] after "
                                        + waitTime + " ms");
    }

    private ConsumerGroupManagementService getConsumerGroupManagementService() throws Exception {
        KafkaAdminClientConfig adminClientConfig = getKafkaAdminClientConfig();
        AdminClient adminClient = AdminClient.create(adminClientConfig.getConfig());
        KafkaManagementConfig kafkaManagementConfig = getKafkaManagementConfig();

        BrokerManagementService brokerManagementService = new BrokerManagementService(adminClient,
                adminClientConfig, kafkaManagementConfig);
        TopicManagementService topicManagementService = new TopicManagementService(brokerManagementService,
                adminClient, adminClientConfig, kafkaManagementConfig);

        KafkaMetricsConfig kafkaMetricsConfig = new KafkaMetricsConfig(MockMetricService.class.getName(),
                                                                       3000L,
                                                                       3000L,
                                                                       1800_000L,
                                                                       1800_000L,
                                                                       20,
                                                                       adminClientConfig.getConfig());

        KafkaConsumerConfig kafkaConsumerConfig =
                getConsumerConfig(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        ConsumerGroupsService consumerGroupsService = new ConsumerGroupsService(adminClientConfig,
                                                                                kafkaConsumerConfig,
                                                                                kafkaMetricsConfig,
                                                                                topicManagementService);

        return new ConsumerGroupManagementService(kafkaMetricsConfig, consumerGroupsService, new MockMetricService());
    }

    @Test
    public void testManuallyAssignedPartitionConsumerGroups() throws Exception {
        String prefix = testName.getMethodName();
        String topicName = prefix + "-topic";
        int partitions = 3;
        CLUSTER.createTopic(topicName, partitions, 1);

        //produce some data
        int msgsCount = 20;
        List<String> produceMessages =
                IntStream.range(0, msgsCount).boxed().map(x -> "message-" + x).collect(Collectors.toList());
        produceData(topicName, produceMessages);
        ConsumerGroupExecutor executor = null;
        ConsumerGroupManagementService consumerGroupManagementService = null;
        String groupId = prefix + "-group";
        try {
            executor = new ConsumerGroupExecutor(1, 1, topicName, groupId, false);
            consumerGroupManagementService = getConsumerGroupManagementService();

            executor.waitTillMsgCount(msgsCount, DEFAULT_WAIT_TIME_MS);
            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();

            Assert.assertTrue(consumerGroupManagementService.consumerGroupNames().contains(groupId));

            ConsumerGroupInfo consumerGroupInfo = consumerGroupManagementService.consumerGroup(groupId);
            List<TopicPartition> topicPartitions = IntStream.range(0, partitions)
                                                            .boxed()
                                                            .map(x -> new TopicPartition(topicName, x))
                                                            .collect(Collectors.toList());
            Assert.assertTrue(topicPartitions(consumerGroupInfo).containsAll(topicPartitions));

            verifyCommittedOffsets(consumerGroupInfo, 20L);
            verifyTotalLag(consumerGroupInfo, 0L);

            verifyAssignmentsWithDefaults(consumerGroupInfo, 3);
        } finally {
            closeQuietly(executor, consumerGroupManagementService);
        }
    }

    @Test
    public void testConsumerAPIsWithMultiplePartitionsAndMultipleGroups() throws Exception {
        String prefix = testName.getMethodName();
        String topic = prefix + "-topic";
        CLUSTER.createTopic(topic, 3, 1);

        //produce some data
        int msgsCount = 20;
        List<String> produceMessages =
                IntStream.range(0, msgsCount).boxed().map(x -> "message-" + x).collect(Collectors.toList());
        produceData(topic, produceMessages);

        //run two consumer groups
        String group1 = prefix + "-group1";
        String group2 = prefix + "-group2";

        ConsumerGroupExecutor executor1 = null;
        ConsumerGroupExecutor executor2 = null;
        ConsumerGroupManagementService consumerGroupManagementService = null;

        try {
            executor1 = new ConsumerGroupExecutor(3, 3, topic, group1);
            executor2 = new ConsumerGroupExecutor(3, 2, topic, group2);
            consumerGroupManagementService = getConsumerGroupManagementService();

            waitTillStable(consumerGroupManagementService, group1, 500L);
            waitTillStable(consumerGroupManagementService, group2, 500L);

            executor1.waitTillMsgCount(msgsCount, DEFAULT_WAIT_TIME_MS);
            executor2.waitTillMsgCount(msgsCount, DEFAULT_WAIT_TIME_MS);

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();

            Assert.assertTrue(consumerGroupManagementService.consumerGroupNames().containsAll(Arrays.asList(group1, group2)));

            ConsumerGroupInfo consumerGroupInfo1 = consumerGroupManagementService.consumerGroup(group1);
            ConsumerGroupInfo consumerGroupInfo2 = consumerGroupManagementService.consumerGroup(group2);

            List<TopicPartition> topicPartitions = new ArrayList<>();
            IntStream.range(0, 3).forEach(x -> topicPartitions.add(new TopicPartition(topic, x)));

            Assert.assertTrue(topicPartitions(consumerGroupInfo1).containsAll(topicPartitions));
            Assert.assertTrue(topicPartitions(consumerGroupInfo2).containsAll(topicPartitions));

            verifyTotalLag(consumerGroupInfo1, 0L);
            verifyTotalLag(consumerGroupInfo2, 0L);

            verifyCommittedOffsets(consumerGroupInfo1, 20L);
            verifyCommittedOffsets(consumerGroupInfo2, 20L);

            verifyUniqueClientIds(consumerGroupInfo1, 3);
            verifyUniqueClientIds(consumerGroupInfo2, 2);

            long allConsumersSize = consumerGroupManagementService.allConsumerInfo()
                                                  .stream()
                                                  .filter(c -> !c.clientId().equals("-")).count();
            Assert.assertTrue(allConsumersSize > 0 && allConsumersSize <= 5);

        } finally {
            closeQuietly(executor1, executor2, consumerGroupManagementService);
        }
    }

    @Ignore
    public void testActiveInactiveGroupApisByState() throws Exception {
        String prefix = testName.getMethodName();
        String topic = prefix + "-topic";
        CLUSTER.createTopic(topic, 3, 1);

        //produce some data
        int msgsCount = 20;
        List<String> produceMessages =
                IntStream.range(0, msgsCount).boxed().map(x -> "message-" + x).collect(Collectors.toList());
        produceData(topic, produceMessages);


        final ConsumerGroupManagementService consumerGroupManagementService = getConsumerGroupManagementService();
        List<ConsumerGroupExecutor> activeGroupExecutors = null;
        try {
            // create 2 groups which remain active.
            int activeGroupsCt = 2;
            String activeGroupFormat = prefix + "-active-group-%s";
            activeGroupExecutors =
                    createConsumerGroupExecutors(topic, activeGroupsCt, activeGroupFormat);
            Set<String> activeGroupNames =
                    activeGroupExecutors.stream().map(executor -> executor.groupId).collect(Collectors.toSet());

            // create 3 groups which will be inactive
            int inactiveGroupsCt = 3;
            String inactiveGroupFormat = prefix + "-inactive-group-%s";
            List<ConsumerGroupExecutor> inactiveGroupExecutors =
                    createConsumerGroupExecutors(topic, inactiveGroupsCt, inactiveGroupFormat);
            Set<String> inactiveGroupNames =
                    inactiveGroupExecutors.stream().map(executor -> executor.groupId).collect(Collectors.toSet());


            inactiveGroupNames.forEach(name -> waitTillStable(consumerGroupManagementService, name, 100L));

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();

            final Set<String> receivedActiveGroups =
                    fetchGroupdIds(consumerGroupManagementService.consumerGroups(ClientState.active));
            Assertions.assertTrue(receivedActiveGroups.containsAll(activeGroupNames));
            Assertions.assertTrue(receivedActiveGroups.containsAll(inactiveGroupNames));

            inactiveGroupExecutors.forEach(executor -> {
                try {
                    executor.close();
                } catch (Exception e) {
                }
            });

            IntStream.range(0, inactiveGroupsCt).forEach(x -> {
                waitTillEmpty(consumerGroupManagementService, String.format(inactiveGroupFormat, x), 100L);
            });

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();

            Set<String> receivedNewActiveGroups =
                    fetchGroupdIds(consumerGroupManagementService.consumerGroups(ClientState.active));
            Assertions.assertTrue(receivedNewActiveGroups.containsAll(activeGroupNames));


            Set<String> receivedInactiveGroups =
                    fetchGroupdIds(consumerGroupManagementService.consumerGroups(ClientState.inactive));

            Assertions.assertTrue(receivedInactiveGroups.containsAll(inactiveGroupNames));
        } finally {
            closeQuietly(consumerGroupManagementService);
            activeGroupExecutors.forEach( executor -> closeQuietly(executor));
        }
    }

    private Set<String> fetchGroupdIds(Collection<ConsumerGroupInfo> consumerGroupInfos) {
        return consumerGroupInfos.stream()
                                 .map(cg -> cg.id())
                                 .collect(Collectors.toSet());
    }

    private List<ConsumerGroupExecutor> createConsumerGroupExecutors(String topic, int activeGroupsCt,
                                                                     String activeGroupFormat) {
        List<ConsumerGroupExecutor> groupExecutors = new ArrayList<>(activeGroupsCt);
        IntStream.range(0, activeGroupsCt)
                 .forEach( x -> {
                     String name = String.format(activeGroupFormat, x);
                     groupExecutors.add(new ConsumerGroupExecutor(1, 1, topic, name));
                 });

        return groupExecutors;
    }

    private Set<TopicPartition> topicPartitions(ConsumerGroupInfo consumerGroupInfo) {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        consumerGroupInfo.topicPartitionAssignments().forEach((topic, partitionMap) -> {
            partitionMap.keySet().forEach(x -> {
                topicPartitions.add(new TopicPartition(topic, x));
            });
        });

        return topicPartitions;
    }

    private void verifyTotalLag(ConsumerGroupInfo consumerGroupInfo, long lag) {
        Assert.assertEquals(lag, consumerGroupInfo
                .topicPartitionAssignments()
                .values()
                .stream()
                .map(x -> x.values())
                .flatMap(Collection::stream)
                .mapToLong(i -> i.lag())
                .sum());
    }

    private void verifyCommittedOffsets(ConsumerGroupInfo consumerGroupInfo, long offsets) {
        Assert.assertEquals(offsets,
                            consumerGroupInfo
                                    .topicPartitionAssignments()
                                    .values()
                                    .stream()
                                    .map(x -> x.values())
                                    .flatMap(Collection::stream)
                                    .mapToLong(i -> i.offset())
                                    .sum());
    }

    private void verifyAssignmentsWithDefaults(ConsumerGroupInfo consumerGroupInfo, int count) {
        Assert.assertEquals(count, consumerGroupInfo
                .topicPartitionAssignments()
                .values()
                .stream()
                .map(x -> x.values())
                .flatMap(Collection::stream)
                .filter(c ->
                                PartitionAssignment.DEFAULT_CLIENT_ID.equals(c.clientId()) &&
                                PartitionAssignment.DEFAULT_HOST.equals(c.host()) &&
                                PartitionAssignment.DEFAULT_CONSUMER_ID.equals(c.consumerInstanceId()))
                .count());
    }

    private void verifyUniqueClientIds(ConsumerGroupInfo consumerGroupInfo, int maxClients) {
        LOG.info("###### Verifying unique client ids for consumerGroupInfo [{}] ", consumerGroupInfo);
        int uniqueClientIds = getUniqueClientIds(consumerGroupInfo);
        // clients in a group may not be always equally distributed. It may take little time before all groups join and
        // distribute equally. So, we should check only that uniqueClients can be in [1, maxClient] but not always
        // equal to maxClients
        Assertions.assertTrue(uniqueClientIds > 0 && uniqueClientIds <= maxClients);
        LOG.info("Finished verifying unique client ids for consumerGroupInfo [{}], unique clients: [{}] ",
                 consumerGroupInfo, uniqueClientIds);
    }

    private int getUniqueClientIds(ConsumerGroupInfo consumerGroupInfo) {
        return consumerGroupInfo
                .topicPartitionAssignments()
                .values()
                .stream()
                .map(x -> x.values())
                .flatMap(Collection::stream)
                .map(c -> {
                    return c.clientId();
                })
                .collect(Collectors.toSet()).size();
    }

    private void verifyConsumerPartitionsForGroup(ConsumerInfo consumerInfo, String group, int count) {
        Assert.assertEquals(count, consumerInfo.consumerPartitionInfos()
                                               .stream()
                                               .filter(c -> c.getGroupId().equals(group))
                                               .collect(Collectors.toList()).size());
    }

    @Test
    public void testConsumerAPIsWithOffsetLag() throws Exception {
        String prefix = testName.getMethodName();
        String topic1 = prefix + "-topic";
        CLUSTER.createTopic(topic1, 1, 1);

        //produce some data
        int msgsCount = 20;
        List<String> produceMessages =
                IntStream.range(0, msgsCount).boxed().map(x -> "message-" + x).collect(Collectors.toList());
        produceData(topic1, produceMessages);

        //run two consumer groups
        String topicGroup1 = prefix + "-group1";
        KafkaConsumer consumer = null;
        try {
            consumer =
                    new KafkaConsumer(getConsumerConfig(topicGroup1, "test-clientId-" + UUID.randomUUID()).getConfig());
            consumer.subscribe(Collections.singleton(topic1));
            consumer.poll(Long.MAX_VALUE);
            consumer.commitSync();

            ConsumerGroupManagementService consumerGroupManagementService = getConsumerGroupManagementService();
            waitTillStable(consumerGroupManagementService, topicGroup1, 500L);
            ConsumerGroupInfo consumerGroupInfo = consumerGroupManagementService.consumerGroup(topicGroup1);

            verifyTotalLag(consumerGroupInfo, 0L);
            verifyCommittedOffsets(consumerGroupInfo, 20L);
            verifyUniqueClientIds(consumerGroupInfo, 1);

            //send some more messages and don't consume
            produceData(topic1, produceMessages);

            consumerGroupManagementService.waitTillRefreshConsumerGroupInfo();
            consumerGroupInfo = consumerGroupManagementService.consumerGroup(topicGroup1);
            verifyTotalLag(consumerGroupInfo, 20L);
            verifyCommittedOffsets(consumerGroupInfo, 20L);
        } finally {
            closeQuietly(consumer);
        }
    }

    private void produceData(String topic, final List<String> inputValues) throws Exception {
        final Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(topic, inputValues, producerProp, CLUSTER.time);
    }

    private KafkaAdminClientConfig getKafkaAdminClientConfig() {
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        adminProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaAdminClientConfig(CLUSTER.bootstrapServers(), adminProps);
    }

    private Map<String, Object> getAdminConfig() {
        Map<String, Object> consumerProp = new HashMap<>();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put("consumer.group.refresh.interval.ms", "200000");
        return consumerProp;
    }

    private static KafkaConsumerConfig getConsumerConfig(String groupId, String clientId) {
        Map<String, Object> consumerConf = new HashMap<>();
        Map<String, Object> consumerProp = new HashMap<>();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProp.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        consumerProp.put(COMMIT_OFFSETS_REFRESH_INTERVAL_MS, 100);
        consumerConf.put(KafkaConsumerConfig.POLL_TIMEOUT_MS_PROPERTY_NAME, 30000L);
        consumerConf.put("properties", consumerProp);
        return new KafkaConsumerConfig(CLUSTER.bootstrapServers(), "", consumerConf);
    }

    private static class ConsumerRunnable implements Runnable {
        private KafkaConsumer<String, String> consumer;
        private String topic;
        private boolean autoAssignPartitions;
        private AtomicInteger msgsCt = new AtomicInteger();

        public ConsumerRunnable(String topic, String groupId, String clientId, boolean autoAssignPartitions) {
            consumer = new KafkaConsumer<>(getConsumerConfig(groupId, clientId).getConfig());
            this.topic = topic;
            this.autoAssignPartitions = autoAssignPartitions;
        }

        public ConsumerRunnable(String topic, String groupId, String clientId) {
            this(topic, groupId, clientId, true);
        }

        private void subscribe(String topic) {
            if (autoAssignPartitions) {
                consumer.subscribe(Collections.singleton(topic));
            } else {
                consumer.assign(
                        consumer.partitionsFor(topic)
                                .stream()
                                .map(tp -> new org.apache.kafka.common.TopicPartition(topic, tp.partition()))
                                .collect(Collectors.toList())
                );
            }
        }

        @Override
        public void run() {
            try {
                subscribe(topic);
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Long.MAX_VALUE);
                    consumer.commitSync();
                    msgsCt.addAndGet(consumerRecords.count());
                }
            } catch (WakeupException w) {
                // ignore
            } finally {
                consumer.close();
            }
        }

        public int getMsgsCt() {
            return msgsCt.get();
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

    private static class ConsumerGroupExecutor implements AutoCloseable {
        private ExecutorService executor;
        private final String groupId;
        private List<ConsumerRunnable> consumers = new LinkedList<>();

        public ConsumerGroupExecutor(int noOfThreads, int numConsumers, String topic, String groupId) {
            this(noOfThreads, numConsumers, topic, groupId, true);
        }

        public ConsumerGroupExecutor(int noOfThreads,
                                     int numConsumers,
                                     String topic,
                                     String groupId,
                                     boolean autoAssignPartitions) {
            executor = Executors.newFixedThreadPool(noOfThreads);
            this.groupId = groupId;
            IntStream.range(0, numConsumers)
                     .forEach(i -> submit(
                             new ConsumerRunnable(topic, groupId, fetchClientId(groupId, i), autoAssignPartitions)));
        }

        String groupId() {
            return groupId;
        }

        public void submit(ConsumerRunnable consumerRunnable) {
            consumers.add(consumerRunnable);
            executor.submit(consumerRunnable);
        }

        public void waitTillMsgCount(int expectedMsgsCt, long waitTimeMs) throws Exception {
            long startTime = System.currentTimeMillis();
            while (true) {
                int currentMsgsCt = 0;
                for (ConsumerRunnable consumer : consumers) {
                    currentMsgsCt += consumer.getMsgsCt();
                }
                if (currentMsgsCt == expectedMsgsCt) {
                    break;
                }

                Thread.sleep(Math.min(100L, waitTimeMs));

                if (System.currentTimeMillis() - startTime > waitTimeMs) {
                    throw new TimeoutException(
                            "Could not receive messages " + expectedMsgsCt + " in duration " + waitTimeMs + " msecs");
                }
            }
        }

        public void close() {
            consumers.forEach(c -> c.shutdown());
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
        }
    }

    private static String fetchClientId(String groupId, int i) {
        return String.format(CLIENT_ID_FORMAT, groupId, i);
    }

    private KafkaManagementConfig getKafkaManagementConfig() {
        return new KafkaManagementConfig(15000L);
    }

}
