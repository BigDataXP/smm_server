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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.AggregateFunction;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricName;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.PostProcessFunction;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Singleton
public class MetricsCache implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsCache.class);
    private static final String KAFKA_CLUSTER = "KAFKA_CLUSTER";

    private final MetricsFetcher fetcher;
    private RefreshMetricsCacheTask refreshMetricsCacheTask;
    private final Map<TimeSpan.TimePeriod, Map<String, Map<MetricDescriptor, MetricValue>>> metricsCache =
            new HashMap<>();
    private final Map<TimeSpan.TimePeriod, Map<String, Map<MetricDescriptorWildCardResolutionKey,
            List<MetricDescriptor>>>> metricDescriptorWildCardResolutionMap = new HashMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                               .setNameFormat("metrics-cache-poller-%d")
                                                               .setDaemon(true)
                                                               .build());

    public static class MetricDescriptorWildCardResolutionKey {

        private MetricName metricName;

        private AggregateFunction aggregateFunction;

        private PostProcessFunction postProcessFunction;

        public MetricDescriptorWildCardResolutionKey(MetricName metricName,
                                                     AggregateFunction aggregateFunction,
                                                     PostProcessFunction postProcessFunction) {
            this.metricName = metricName;
            this.aggregateFunction = aggregateFunction;
            this.postProcessFunction = postProcessFunction;
        }

        @Override
        public String toString() {
            return "MetricDescriptorWildCardResolutionKey{" +
                    "metricName=" + metricName +
                    ", aggregateFunction=" + aggregateFunction +
                    ", postProcessFunction=" + postProcessFunction +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricDescriptorWildCardResolutionKey that = (MetricDescriptorWildCardResolutionKey) o;
            return Objects.equals(metricName, that.metricName) &&
                    Objects.equals(aggregateFunction, that.aggregateFunction) &&
                    Objects.equals(postProcessFunction, that.postProcessFunction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metricName, aggregateFunction, postProcessFunction);
        }

    }

    static class MetricValue {
        private static final MetricValue EMPTY = new MetricValue(Collections.emptyMap());

        // This pool is used here to reduce the memory foot print of delta timestamp objects in Memory
        private static final Map<Long, Long> deltaTimestampPool = new ConcurrentHashMap<>();

        private Long startTimeMs;
        private Long[] deltaTimestamps;
        private Double[] metrics;

        static void refreshPool(Set<Long> allowedDeltaTimestamps) {
            LOG.info("DeltaTimestampPool size before refresh : {}", deltaTimestampPool.size());
            deltaTimestampPool.entrySet().removeIf(x -> !allowedDeltaTimestamps.contains(x.getKey()));
            LOG.info("DeltaTimestampPool size after refresh : {}", deltaTimestampPool.size());
        }

        MetricValue(Map<Long, Double> map) {
            if (map != null && !map.isEmpty()) {
                TreeMap<Long, Double> treeMap = new TreeMap<>(map);
                startTimeMs = treeMap.entrySet().iterator().next().getKey();
                deltaTimestamps = new Long[treeMap.size()];
                metrics = new Double[treeMap.size()];

                int i=0;
                Long previousTimestamp = startTimeMs;
                for (Map.Entry<Long, Double> e : treeMap.entrySet()) {
                    Long delta = e.getKey() - previousTimestamp;
                    deltaTimestamps[i] = deltaTimestampPool.computeIfAbsent(delta, x -> x);
                    previousTimestamp = e.getKey();
                    metrics[i++] = e.getValue();
                }
            }
        }

        Map<Long, Double> values() {
            Map<Long, Double> map = new LinkedHashMap<>();
            if (startTimeMs != null) {
                int length = deltaTimestamps.length;
                Long previousTimestamp = startTimeMs;
                for (int i=0; i<length; i++) {
                    Long timestamp = previousTimestamp + deltaTimestamps[i];
                    previousTimestamp = timestamp;
                    map.put(timestamp, metrics[i]);
                }
            }
            return map;
        }

        int size() {
            return deltaTimestamps == null ? 0 : deltaTimestamps.length;
        }
    }

    @Inject
    public MetricsCache(MetricsFetcher fetcher,
                        KafkaMetricsConfig config,
                        BrokerManagementService brokerMgmtService) {
        this.fetcher = fetcher;
        if (config != null) {
            int nThreads = config.getMetricsFetcherThreads();
            refreshMetricsCacheTask = new RefreshMetricsCacheTask(brokerMgmtService, nThreads);
            refreshMetricsCacheTask.run();
            long refreshIntervalMs = config.getMetricsCacheRefreshIntervalMs();
            scheduler.scheduleWithFixedDelay(refreshMetricsCacheTask, refreshIntervalMs, refreshIntervalMs,
                    TimeUnit.MILLISECONDS);
            scheduler.scheduleAtFixedRate(new RefreshDeltaTimestampPoolTask(), 1, 1, TimeUnit.DAYS);
            LOG.info("Metric fetcher thread started with cache refresh interval : {} ms", refreshIntervalMs);
        }
    }

    public void refresh() {
        if (refreshMetricsCacheTask != null) {
            refreshMetricsCacheTask.run();
        }
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMetrics(BrokerNode brokerNode,
                                                                     TimeSpan timeSpan,
                                                                     MetricDescriptor metricDescriptor) {
        return getBrokerMetrics(brokerNode, timeSpan, Collections.singleton(metricDescriptor));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMetrics(BrokerNode brokerNode,
                                                                     TimeSpan timeSpan,
                                                                     Collection<MetricDescriptor> metricDescriptors) {
        Objects.requireNonNull(brokerNode, "BrokerNode should not be null");
        return getMetrics(brokerNode, timeSpan, metricDescriptors);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterMetrics(TimeSpan timeSpan,
                                                                      MetricDescriptor metricDescriptor) {
        return getClusterMetrics(timeSpan, Collections.singleton(metricDescriptor));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterMetrics(TimeSpan timeSpan,
                                                                      Collection<MetricDescriptor> metricDescriptors) {
        return getMetrics(null, timeSpan, metricDescriptors);
    }

    @Override
    public void close() throws Exception {
        refreshMetricsCacheTask.close();
        scheduler.shutdown();
        scheduler.awaitTermination(5 * 60 * 1000L, TimeUnit.MILLISECONDS);
        LOG.info("Stopped the Metrics Cache");
    }

    private Map<MetricDescriptor, Map<Long, Double>> getMetrics(BrokerNode brokerNode,
                                                                TimeSpan timeSpan,
                                                                Collection<MetricDescriptor> descriptors) {
        String host = (brokerNode != null) ? brokerNode.host() : KAFKA_CLUSTER;
        Map<MetricDescriptor, Map<Long, Double>> metrics = new HashMap<>();
        if (timeSpan.readFromCache()) {
            List<MetricDescriptor> metricDescriptorsNotInCache = new ArrayList<>();
            Lock lock = readWriteLock.readLock();
            lock.lock();
            try {
                Map<String, Map<MetricDescriptor, MetricValue>> hostToMetricResponseMap = metricsCache.get(timeSpan.timePeriod());
                Map<MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> metricResolutionMap
                        = metricDescriptorWildCardResolutionMap.computeIfAbsent(timeSpan.timePeriod(), timePeriod -> Collections.emptyMap())
                        .computeIfAbsent(host, hostName -> Collections.emptyMap());
                if (hostToMetricResponseMap != null) {
                    Map<MetricDescriptor, MetricValue> responseMap = hostToMetricResponseMap.get(host);
                    if (responseMap != null && !responseMap.isEmpty()) {
                        for (MetricDescriptor descriptor : descriptors) {
                            if (responseMap.containsKey(descriptor)) {
                                metrics.put(descriptor, responseMap.get(descriptor).values());
                            } else if (descriptor.queryTags().values().contains(MetricsService.WILD_CARD)) {
                                List<MetricDescriptor> matchedDescriptors = findAllMatchingDescriptors(metricResolutionMap,
                                        descriptor);
                                if (!matchedDescriptors.isEmpty()) {
                                    for (MetricDescriptor matchedDescriptor : matchedDescriptors) {
                                        metrics.put(matchedDescriptor, responseMap.get(matchedDescriptor).values());
                                    }
                                } else {
                                    metricDescriptorsNotInCache.add(descriptor);
                                }
                            } else {
                                metricDescriptorsNotInCache.add(descriptor);
                            }
                        }
                    } else {
                        metricDescriptorsNotInCache.addAll(descriptors);
                    }
                } else {
                    metricDescriptorsNotInCache.addAll(descriptors);
                }
            } finally {
                lock.unlock();
            }
            if (!metricDescriptorsNotInCache.isEmpty()) {
                LOG.warn("Descriptors are not available in cache : {}", metricDescriptorsNotInCache);
            }
        } else {
            if (brokerNode != null) {
                Map<MetricDescriptor, Map<Long, Double>> brokerMetrics = fetcher.getBrokerMetrics(brokerNode,
                        timeSpan.startTimeMs(),
                        timeSpan.endTimeMs(),
                        descriptors);
                metrics.putAll(brokerMetrics);
            } else {
                Map<MetricDescriptor, Map<Long, Double>> clusterMetrics = fetcher.getClusterMetrics(
                        timeSpan.startTimeMs(),
                        timeSpan.endTimeMs(),
                        descriptors);
                metrics.putAll(clusterMetrics);
            }
        }
        return metrics;
    }

    /**
     * Returns all the matching cached descriptors for the given descriptor.
     * NOTE: It's assumed that all the tags would be provided with proper / wild-card value.
     *
     * @param metricDescriptorMap metric descriptor with one or more wild card tags to resolved metric descriptor map
     * @param descriptor          descriptor to match
     * @return all the descriptor that matched with the given descriptor.
     */
    List<MetricDescriptor> findAllMatchingDescriptors(Map<MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> metricDescriptorMap,
                                                      MetricDescriptor descriptor) {
        MetricDescriptorWildCardResolutionKey key = new MetricDescriptorWildCardResolutionKey(descriptor.metricName(),
                descriptor.aggrFunction(),
                descriptor.postProcessFunction());
        List<MetricDescriptor> resolvedMetricDescriptors = metricDescriptorMap.get(key);
        List<MetricDescriptor> matchedDescriptors = new ArrayList<>();
        if (resolvedMetricDescriptors != null && !resolvedMetricDescriptors.isEmpty()) {
            for (MetricDescriptor cachedDescriptor : resolvedMetricDescriptors) {
                boolean isMatch = true;
                for (Map.Entry<String, String> entry : descriptor.queryTags().entrySet()) {
                    String searchQueryTagKey = entry.getKey();
                    String searchQueryTagValue = entry.getValue();
                    String cachedQueryTagValue = cachedDescriptor.queryTags().get(searchQueryTagKey);

                    if (!searchQueryTagValue.equals(MetricsService.WILD_CARD) &&
                            !searchQueryTagValue.equals(cachedQueryTagValue)) {
                        isMatch = false;
                    }

                    if (!isMatch) {
                        break;
                    }
                }

                if (isMatch) {
                    matchedDescriptors.add(cachedDescriptor);
                }
            }
        } else {
            LOG.warn("Couldn't resolve metric descriptors for {} ", descriptor);
        }
        return matchedDescriptors;
    }

    private class RefreshMetricsCacheTask implements Runnable {

        private final BrokerManagementService brokerMgmtService;
        private final MetricDescriptorSupplier supplier;
        private final ExecutorService executors;
        private final AtomicBoolean isRunning;

        private RefreshMetricsCacheTask(BrokerManagementService brokerMgmtService, int nThreads) {
            this.brokerMgmtService = brokerMgmtService;
            this.supplier = fetcher.getMetricDescriptorSupplier();
            this.executors = Executors.newFixedThreadPool(nThreads,
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("metrics-fetcher-thread-%d").build());
            this.isRunning = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            if (!isRunning.compareAndSet(false, true)) {
                LOG.info("Ignoring the redundant call to refresh the metrics cache");
                return;
            }

            try {
                long startTimeMs = System.currentTimeMillis();
                List<Future<?>> futures = new ArrayList<>();
                for (TimeSpan.TimePeriod timePeriod : TimeSpan.TimePeriod.values()) {
                    Future<?> future = executors.submit(() -> refreshMetrics(timePeriod));
                    futures.add(future);
                }

                for (Future<?> future : futures) {
                    future.get();
                }
                LOG.info("Size of Metric deltaTimestamp : {}", MetricValue.deltaTimestampPool.size());
                LOG.info("Updated the Metrics cache. Time taken to fetch all metrics from AMS : {} ms",
                        (System.currentTimeMillis() - startTimeMs));
            } catch (Exception ex) {
                LOG.error("Error while refreshing the metrics cache", ex);
            } finally {
                isRunning.set(false);
            }
        }

        public void close() throws InterruptedException {
            executors.shutdown();
            executors.awaitTermination(300, TimeUnit.SECONDS);
        }

        private void refreshMetrics(TimeSpan.TimePeriod timePeriod) {
            long startTimeMs = System.currentTimeMillis();
            try {
                for (BrokerNode brokerNode : brokerMgmtService.allBrokers()) {
                    Map<MetricDescriptor, MetricValue> brokerMetrics = refreshBrokerMetrics(brokerNode, timePeriod);
                    Map<MetricDescriptor, MetricValue> brokerHostMetrics = refreshBrokerHostMetrics(brokerNode, timePeriod);
                    if (brokerMetrics != null && brokerHostMetrics != null) {
                        long beginTimeMs = System.currentTimeMillis();
                        Lock lock = readWriteLock.writeLock();
                        lock.lock();
                        try {
                            Map<MetricDescriptor, MetricValue> brokerCache =
                                    metricsCache.computeIfAbsent(timePeriod, period -> new HashMap<>())
                                            .computeIfAbsent(brokerNode.host(), host -> new HashMap<>());
                            brokerCache.clear();
                            brokerCache.putAll(brokerMetrics);
                            brokerCache.putAll(brokerHostMetrics);

                            Map<MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> brokerLevelMetricsDescriptorWildCardResolutionMap =
                                    metricDescriptorWildCardResolutionMap.computeIfAbsent(timePeriod, period -> new HashMap<>())
                                            .computeIfAbsent(brokerNode.host(), host -> new HashMap<>());
                            brokerLevelMetricsDescriptorWildCardResolutionMap.clear();
                            populateMetricDescriptorWildCardResolutionMap(brokerLevelMetricsDescriptorWildCardResolutionMap, brokerMetrics);
                            populateMetricDescriptorWildCardResolutionMap(brokerLevelMetricsDescriptorWildCardResolutionMap ,brokerHostMetrics);
                        } finally {
                            lock.unlock();
                        }
                        LOG.debug("Time taken to obtain write lock and update brokerId : {} metrics in cache : {} ms",
                                brokerNode.id(), (System.currentTimeMillis() - beginTimeMs));
                    }
                }

                List<MetricDescriptor> clusterMetricDescriptors = new ArrayList<>();
                clusterMetricDescriptors.addAll(supplier.getTopicMetricDescriptors("%"));
                clusterMetricDescriptors.addAll(supplier.getTopicPartitionMetricDescriptors("%"));
                clusterMetricDescriptors.addAll(supplier.getClusterMetricDescriptors());
                clusterMetricDescriptors.addAll(supplier.getConsumerMetricDescriptors());

                Map<MetricDescriptor, MetricValue> clusterMetrics = refreshClusterMetrics(timePeriod, clusterMetricDescriptors);
                if (clusterMetrics != null) {
                    long beginTimeMs = System.currentTimeMillis();
                    Lock lock = readWriteLock.writeLock();
                    lock.lock();
                    try {
                        Map<MetricDescriptor, MetricValue> clusterCache =
                                metricsCache.computeIfAbsent(timePeriod, period -> new HashMap<>())
                                        .computeIfAbsent(KAFKA_CLUSTER, cluster -> new HashMap<>());
                        clusterCache.clear();
                        clusterCache.putAll(clusterMetrics);

                        Map<MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> clusterLevelMetricsDescriptorWildCardResolutionMap =
                                metricDescriptorWildCardResolutionMap.computeIfAbsent(timePeriod, period -> new HashMap<>())
                                        .computeIfAbsent(KAFKA_CLUSTER, host -> new HashMap<>());
                        clusterLevelMetricsDescriptorWildCardResolutionMap.clear();
                        populateMetricDescriptorWildCardResolutionMap(clusterLevelMetricsDescriptorWildCardResolutionMap, clusterMetrics);
                    } finally {
                        lock.unlock();
                    }
                    LOG.debug("Time taken to obtain write lock and update cluster metrics in cache : {} ms",
                            (System.currentTimeMillis() - beginTimeMs));
                }
            } catch (Exception ex) {
                LOG.error("TimePeriod : {}, Error while refreshing the metrics cache", timePeriod, ex);
            }

            LOG.info("TimePeriod : {}, Time taken to fetch metrics : {} ms", timePeriod,
                    (System.currentTimeMillis() - startTimeMs));
        }

        private Map<MetricDescriptor, MetricValue> refreshBrokerMetrics(BrokerNode brokerNode,
                                                                        TimeSpan.TimePeriod timePeriod) {
            List<Future<Map<MetricDescriptor, Map<Long, Double>>>> futures = new ArrayList<>();
            for (MetricDescriptor descriptor : supplier.getBrokerMetricDescriptors()) {
                futures.add(executors.submit(() -> {
                    try {
                        return fetcher.getBrokerMetrics(brokerNode, timePeriod.startTimeMillis(),
                                timePeriod.endTimeMillis(), Collections.singleton(descriptor));
                    } catch (Exception ex) {
                        LOG.error("TimePeriod : {}, Error while fetching brokerId : {} metrics : {}",
                                timePeriod, brokerNode.id(), descriptor, ex);
                        return null;
                    }
                }));
            }
            final Map<MetricDescriptor, MetricValue> brokerMetrics = getMetrics(futures);
            Optional<Integer> nMetricPoints = brokerMetrics.values().stream().map(MetricValue::size).reduce((a, b) -> a + b);
            LOG.info("TimePeriod : {}, BrokerId : {} fetched {} descriptors with {} metric points", timePeriod,
                    brokerNode.id(), brokerMetrics.size(), nMetricPoints.orElse(0));
            return brokerMetrics;
        }

        private Map<MetricDescriptor, MetricValue> refreshBrokerHostMetrics(BrokerNode brokerNode,
                                                                            TimeSpan.TimePeriod timePeriod) {
            List<Future<Map<MetricDescriptor, Map<Long, Double>>>> futures = new ArrayList<>();
            for (MetricDescriptor descriptor : supplier.getBrokerHostMetricDescriptors()) {
                futures.add(executors.submit(() -> {
                    try {
                        return fetcher.getHostMetrics(brokerNode, timePeriod.startTimeMillis(),
                                timePeriod.endTimeMillis(), Collections.singleton(descriptor));
                    } catch (Exception ex) {
                        LOG.error("TimePeriod : {}, Error while fetching brokerId : {} host metrics : {}",
                                timePeriod, brokerNode.id(), descriptor, ex);
                        return null;
                    }
                }));
            }

            Map<MetricDescriptor, MetricValue> brokerHostMetrics = getMetrics(futures);

            MetricValue memFree = brokerHostMetrics.remove(supplier.memFree());
            MetricValue memTotal = brokerHostMetrics.remove(supplier.memTotal());
            MetricValue memFreePercent = calculateMemFreePercent(memFree, memTotal);
            brokerHostMetrics.put(supplier.memFreePercent(), memFreePercent);

            Optional<Integer> nMetricPoints = brokerHostMetrics.values().stream().map(MetricValue::size).reduce((a, b) -> a + b);
            LOG.info("TimePeriod : {}, BrokerId : {} fetched {} descriptors with {} host metric points",
                    timePeriod, brokerNode.id(), brokerHostMetrics.size(), nMetricPoints.orElse(0));
            return brokerHostMetrics;
        }

        private void populateMetricDescriptorWildCardResolutionMap(Map<MetricDescriptorWildCardResolutionKey, List<MetricDescriptor>> metricsDescriptorWildCardResolutionMap,
                                                                   Map<MetricDescriptor, MetricValue> metrics) {
            for (Map.Entry<MetricDescriptor, MetricValue> metricEntry : metrics.entrySet()) {

                MetricDescriptor metricDescriptor = metricEntry.getKey();
                MetricDescriptorWildCardResolutionKey key = new MetricDescriptorWildCardResolutionKey(metricDescriptor.metricName(),
                                                                                                      metricDescriptor.aggrFunction(),
                                                                                                      metricDescriptor.postProcessFunction());
                List<MetricDescriptor> metricDescriptors = metricsDescriptorWildCardResolutionMap.computeIfAbsent(key, missingKey -> new ArrayList<>());
                metricDescriptors.add(metricDescriptor);
            }
        }

        private MetricValue calculateMemFreePercent(MetricValue memFreeMetricValue,
                                                    MetricValue memTotalMetricValue) {
            if (memFreeMetricValue == null || memTotalMetricValue == null) {
                return MetricValue.EMPTY;
            }

            Map<Long, Double> memFree = memFreeMetricValue.values();
            Map<Long, Double> memTotal = memTotalMetricValue.values();

            if (memFree.isEmpty() || memTotal.isEmpty()) {
                return MetricValue.EMPTY;
            }

            Double memTotalValue = memTotal.values().iterator().next();
            if (memTotalValue == null) {
                return MetricValue.EMPTY;
            }

            Map<Long, Double> memFreePercent = new HashMap<>();
            memFree.forEach((timestamp, memFreeValue) -> {
                if (memFreeValue != null) {
                    Double memFreePercentValue = memFreeValue * 100D / memTotalValue;
                    String preciseValue = String.format("%.02f", memFreePercentValue);
                    memFreePercent.put(timestamp, Double.parseDouble(preciseValue));
                }
            });
            return new MetricValue(memFreePercent);
        }

        private Map<MetricDescriptor, MetricValue> refreshClusterMetrics(TimeSpan.TimePeriod timePeriod,
                                                                         Collection<MetricDescriptor> metricDescriptors) {
            List<Future<Map<MetricDescriptor, Map<Long, Double>>>> futures = new ArrayList<>();
            for (MetricDescriptor descriptor : metricDescriptors) {
                futures.add(executors.submit(() -> {
                    try {
                        return fetcher.getClusterMetrics(timePeriod.startTimeMillis(), timePeriod.endTimeMillis(),
                                Collections.singleton(descriptor));
                    } catch (Exception ex) {
                        LOG.error("TimePeriod : {}, Error while fetching cluster metrics : {}",
                                timePeriod, descriptor, ex);
                        return null;
                    }
                }));
            }
            Map<MetricDescriptor, MetricValue> clusterMetrics = getMetrics(futures);
            Optional<Integer> nMetricPoints = clusterMetrics.values().stream().map(MetricValue::size).reduce((a, b) -> a + b);
            LOG.info("TimePeriod : {}, {} fetched {} descriptors with {} metric points",
                    timePeriod, KAFKA_CLUSTER, clusterMetrics.size(), nMetricPoints.orElse(0));
            return clusterMetrics;
        }

        private Map<MetricDescriptor, MetricValue> getMetrics(
                List<Future<Map<MetricDescriptor, Map<Long, Double>>>> futures) {
            final Map<MetricDescriptor, MetricValue> metrics = new HashMap<>();
            for (Future<Map<MetricDescriptor, Map<Long, Double>>> future : futures) {
                try {
                    Map<MetricDescriptor, Map<Long, Double>> result = future.get();
                    if (result != null) {
                        result.forEach((k, v) -> metrics.put(k, new MetricValue(v)));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Error while fetching the metrics", e);
                }
            }
            return metrics;
        }
    }

    private class RefreshDeltaTimestampPoolTask implements Runnable {

        @Override
        public void run() {
            Lock lock = readWriteLock.readLock();
            lock.lock();
            try {
                long startTimeMs = System.currentTimeMillis();
                Set<Long> allowedDeltaTimestamps = new HashSet<>();
                for (Map.Entry<TimeSpan.TimePeriod, Map<String, Map<MetricDescriptor, MetricValue>>> e : metricsCache.entrySet()) {
                    for (Map.Entry<String, Map<MetricDescriptor, MetricValue>> e1 : e.getValue().entrySet()) {
                        for (Map.Entry<MetricDescriptor, MetricValue> e2 : e1.getValue().entrySet()) {
                            Long[] deltaTimestamps = e2.getValue().deltaTimestamps;
                            if (deltaTimestamps != null) {
                                Collections.addAll(allowedDeltaTimestamps, deltaTimestamps);
                            }
                        }
                    }
                }
                MetricValue.refreshPool(allowedDeltaTimestamps);
                LOG.info("Time taken to refresh the deltaTimestampPool : {} ms", (System.currentTimeMillis() - startTimeMs));
            } finally {
                lock.unlock();
            }
        }
    }

}
