package com.hortonworks.smm.kafka.services.metric;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractMetricDescriptorSupplier implements MetricDescriptorSupplier {

    // group names
    protected static final String KAFKA_CLUSTER = "kafka.cluster";
    protected static final String KAFKA_CONTROLLER = "kafka.controller";
    protected static final String KAFKA_COORDINATOR_GROUP = "kafka.coordinator.group";
    protected static final String KAFKA_COORDINATOR_TRANSACTION = "kafka.coordinator.transaction";
    protected static final String KAFKA_LOG = "kafka.log";
    protected static final String KAFKA_NETWORK = "kafka.network";
    protected static final String KAFKA_SERVER = "kafka.server";
    protected static final String KAFKA_UTILS = "kafka.utils";

    // type names
    protected static final String PARTITION = "Partition";
    protected static final String CONTROLLER_CHANNEL_MANAGER = "ControllerChannelManager";
    protected static final String CONTROLLER_STATS = "ControllerStats";
    protected static final String KAFKA_CONTROLLER_TYPE = "KafkaController";
    protected static final String GROUP_METADATA_MANAGER = "GroupMetadataManager";
    protected static final String TRANSACTION_MARKER_CHANNEL_MANAGER = "TransactionMarkerChannelManager";
    protected static final String LOG = "Log";
    protected static final String LOG_CLEANER = "LogCleaner";
    protected static final String LOG_CLEANER_MANAGER = "LogCleanerManager";
    protected static final String LOG_MANAGER = "LogManager";
    protected static final String PROCESSOR = "Processor";
    protected static final String SOCKET_SERVER = "SocketServer";
    protected static final String KAFKA_REQUEST_HANDLER_POOL = "KafkaRequestHandlerPool";
    protected static final String KAFKA_SERVER_TYPE = "KafkaServer";
    protected static final String REQUEST_CHANNEL = "RequestChannel";
    protected static final String REQUEST_METRICS = "RequestMetrics";
    protected static final String BROKER_TOPIC_METRICS = "BrokerTopicMetrics";
    protected static final String BROKER_CLIENT_METRICS = "BrokerClientMetrics";
    protected static final String CONSUMER_GROUP_METRICS = "ConsumerGroupMetrics";
    protected static final String DELAYED_FETCH_METRICS = "DelayedFetchMetrics";
    protected static final String REPLICA_FETCHER_MANAGER = "ReplicaFetcherManager";
    protected static final String REPLICA_MANAGER = "ReplicaManager";
    protected static final String FETCHER_LAG_METRICS = "FetcherLagMetrics";
    protected static final String FETCHER_STATS = "FetcherStats";
    protected static final String SESSION_EXPIRE_LISTENER = "SessionExpireListener";
    protected static final String ZOOKEEPER_CLIENT_METRICS = "ZooKeeperClientMetrics";
    protected static final String THROTTLER = "Throttler";

    // names
    protected static final String MESSAGES_IN_PER_SEC = "MessagesInPerSec";
    protected static final String BYTES_IN_PER_SEC = "BytesInPerSec";
    protected static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
    protected static final String FAILED_PRODUCE_REQUESTS_PER_SEC = "FailedProduceRequestsPerSec";
    protected static final String FAILED_FETCH_REQUESTS_PER_SEC = "FailedFetchRequestsPerSec";
    protected static final String IN_SYNC_REPLICAS_COUNT = "InSyncReplicasCount";
    protected static final String LAG = "Lag";
    protected static final String COMMITTED_OFFSET = "CommittedOffset";
    protected static final String CPU_IDLE = "cpu_idle";
    protected static final String LOAD_FIVE = "load_five";
    protected static final String MEM_FREE = "mem_free";
    protected static final String MEM_TOTAL = "mem_total";
    protected static final String MEM_FREE_PERCENT = "mem_free_percent";
    protected static final String DISK_PERCENT = "disk_percent";
    protected static final String DISK_WRITE_BPS = "write_bps";
    protected static final String DISK_READ_BPS = "read_bps";
    protected static final String TOTAL_PRODUCE_REQUESTS_PER_SEC = "TotalProduceRequestsPerSec";
    protected static final String TOTAL_FETCH_REQUESTS_PER_SEC = "TotalFetchRequestsPerSec";
    protected static final String ACTIVE_CONTROLLER_COUNT = "ActiveControllerCount";
    protected static final String UNCLEAN_LEADER_ELECTIONS_PER_SEC = "UncleanLeaderElectionsPerSec";
    protected static final String REQUEST_HANDLER_AVG_IDLE_PERCENT = "RequestHandlerAvgIdlePercent";
    protected static final String OFFLINE_PARTITIONS_COUNT = "OfflinePartitionsCount";

    // TODO: fill the remaining valid names

    // attribute names
    protected static final String ONE_MINUTE_RATE = "1MinuteRate";
    protected static final String FIVE_MINUTE_RATE = "5MinuteRate";
    protected static final String FIFTEEN_MINUTE_RATE = "15MinuteRate";
    protected static final String SEVENTY_FIVE_PERCENTILE = "75Percentile";
    protected static final String NINETY_FIVE_PERCENTILE = "95Percentile";
    protected static final String NINETY_EIGHT_PERCENTILE = "98Percentile";
    protected static final String NINETY_NINE_PERCENTILE = "99Percentile";
    protected static final String TRIPLE_NINE_PERCENTILE = "999Percentile";
    protected static final String COUNT = "count";
    protected static final String MAX = "max";
    protected static final String MEAN = "mean";
    protected static final String MEAN_RATE = "meanRate";
    protected static final String MEDIAN = "median";
    protected static final String MIN = "min";
    protected static final String STD_DEV = "stddev";

    public static final String CONSUMER_GROUP = "group";
    public static final String CLIENT_ID = "clientId";
    public static final String TOPIC = "topic";
    public static final String PARTITION_NUMBER = "partition";

    protected static final String EMPTY_ATTR = "";
    protected static final List<String> EMPTY_TAG = Collections.emptyList();
    public static final List<String> TOPIC_TAG = Collections.singletonList(TOPIC);
    public static final List<String> TOPIC_PARTITION_TAG = Collections.unmodifiableList(Arrays.asList(PARTITION_NUMBER, TOPIC));
    public static final List<String> PRODUCER_TP_TAG = Collections.unmodifiableList(Arrays.asList(CLIENT_ID, PARTITION_NUMBER, TOPIC));
    public static final List<String> GROUP_TAG = Collections.singletonList(CONSUMER_GROUP);
    public static final List<String> CONSUMER_GROUP_TAG = Collections.unmodifiableList(Arrays.asList(CLIENT_ID, CONSUMER_GROUP, PARTITION_NUMBER, TOPIC));

    public static final List<String> GROUP_NAMES = Collections.unmodifiableList(Arrays.asList(KAFKA_CLUSTER,
            KAFKA_CONTROLLER, KAFKA_COORDINATOR_GROUP, KAFKA_COORDINATOR_TRANSACTION, KAFKA_LOG, KAFKA_NETWORK,
            KAFKA_SERVER, KAFKA_UTILS));

    public static final List<String> TYPE_NAMES = Collections.unmodifiableList(Arrays.asList(PARTITION,
            CONTROLLER_CHANNEL_MANAGER, CONTROLLER_STATS, KAFKA_CONTROLLER_TYPE, GROUP_METADATA_MANAGER,
            TRANSACTION_MARKER_CHANNEL_MANAGER, LOG, LOG_CLEANER, LOG_CLEANER_MANAGER, LOG_MANAGER, PROCESSOR,
            SOCKET_SERVER, KAFKA_REQUEST_HANDLER_POOL, KAFKA_SERVER_TYPE, REQUEST_CHANNEL, REQUEST_METRICS,
            BROKER_TOPIC_METRICS, DELAYED_FETCH_METRICS, REPLICA_FETCHER_MANAGER, REPLICA_MANAGER, FETCHER_LAG_METRICS,
            FETCHER_STATS, SESSION_EXPIRE_LISTENER, ZOOKEEPER_CLIENT_METRICS, THROTTLER));

    public static final List<String> NAMES = Collections.unmodifiableList(Arrays.asList(MESSAGES_IN_PER_SEC,
            BYTES_IN_PER_SEC, BYTES_OUT_PER_SEC, FAILED_PRODUCE_REQUESTS_PER_SEC, FAILED_FETCH_REQUESTS_PER_SEC));

    public static final List<String> ATTRIBUTES = Collections.unmodifiableList(Arrays.asList(ONE_MINUTE_RATE,
            FIVE_MINUTE_RATE, FIFTEEN_MINUTE_RATE, SEVENTY_FIVE_PERCENTILE, NINETY_FIVE_PERCENTILE,
            NINETY_EIGHT_PERCENTILE, NINETY_NINE_PERCENTILE, TRIPLE_NINE_PERCENTILE, COUNT, MAX, MEAN, MEAN_RATE,
            MEDIAN, MIN, STD_DEV));

    public abstract MetricDescriptor memFree();

    public abstract MetricDescriptor memTotal();

    public abstract MetricDescriptor memFreePercent();

    public abstract MetricDescriptor diskPercent();

    public abstract MetricDescriptor diskWriteBps();

    public abstract MetricDescriptor diskReadBps();
}
