package com.alibaba.flink.connectors.dts;

import com.alibaba.flink.connectors.dts.fetcher.DtsKafkaUtil;
import com.alibaba.flink.connectors.dts.internal.DtsKafkaFetcher;
import com.alibaba.flink.connectors.dts.util.DtsUtil;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitModes;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.*;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import static org.apache.flink.util.PropertiesUtil.getBoolean;

public class FlinkDtsKafkaConsumer<T> extends FlinkDtsConsumer<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkDtsKafkaConsumer.class);

    /** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks. */
    public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

    /**  Configuration key to change the polling timeout. **/
    public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

    /** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
     * available. If 0, returns immediately with any records that are available now. */
    public static final long DEFAULT_POLL_TIMEOUT = 100L;

    /**
     * The default interval to execute partition discovery,
     * in milliseconds ({@code Long.MIN_VALUE}, i.e. disabled by default).
     */
    public static final long PARTITION_DISCOVERY_DISABLED = Long.MIN_VALUE;

    /** Boolean configuration key to disable metrics tracking. **/
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /** Configuration key to define the consumer's partition discovery interval, in milliseconds. */
    public static final String KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "flink.partition-discovery.interval-millis";

    /** State name of the consumer's partition offset states. */
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    /** Invalid timestamp for the state */
    public static final long INVALID_TIMESTAMP = Long.MIN_VALUE;

    // ------------------------------------------------------------------------
    //  configuration state, set on the client relevant for all subtasks
    // ------------------------------------------------------------------------

    /** Describes whether we are discovering partitions for fixed topics or a topic pattern. */
    private final KafkaTopicsDescriptor topicsDescriptor;

    /** The schema to convert between Kafka's byte messages, and Flink's objects. */
    protected final KafkaDeserializationSchema<T> deserializer;

    /** The set of topic partitions that the source will read, with their initial offsets to start reading from. */
    private Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets;

    /**
     * Optional watermark strategy that will be run per Kafka partition, to exploit per-partition
     * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
     * it into multiple copies.
     */
    private SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

    /** The set of topic partitions that the source will read, with their initial offsets to start reading from. */
    private Map<KafkaTopicPartition, Long> subscribedPartitionsToEndOffsets;

    /** Finish a partition when newest message has been processed. */
    private boolean stopAtLatest = false;

    /** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
     * to exploit per-partition timestamp characteristics.
     * The assigner is kept in serialized form, to deserialize it into multiple copies. */
    private SerializedValue<AssignerWithPeriodicWatermarks<T>> periodicWatermarkAssigner;

    /** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
     * to exploit per-partition timestamp characteristics.
     * The assigner is kept in serialized form, to deserialize it into multiple copies. */
    private SerializedValue<AssignerWithPunctuatedWatermarks<T>> punctuatedWatermarkAssigner;

    /**
     * User-set flag determining whether or not to commit on checkpoints.
     * Note: this flag does not represent the final offset commit mode.
     */
    private boolean enableCommitOnCheckpoints = true;

    /** User-set flag to disable filtering restored partitions with current topics descriptor. */
    private boolean filterRestoredPartitionsWithCurrentTopicsDescriptor = true;

    /**
     * The offset commit mode for the consumer.
     * The value of this can only be determined since it depends
     * on whether or not checkpointing is enabled for the job.
     */
    private OffsetCommitMode offsetCommitMode;

    /** User configured value for discovery interval, in milliseconds. */
    private final long discoveryIntervalMillis;

    /** The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
    private StartupMode startupMode = StartupMode.GROUP_OFFSETS;

    /** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
    private Map<KafkaTopicPartition, Long> specificStartupOffsets;

    /** Timestamp to determine startup offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}. */
    private Long startupOffsetsTimestamp;

    // ------------------------------------------------------------------------
    //  runtime state (used individually by each parallel subtask)
    // ------------------------------------------------------------------------

    /** Data for pending but uncommitted offsets. */
    private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

    /** The fetcher implements the connections to the Kafka brokers. */
    private transient volatile DtsKafkaFetcher<T> kafkaFetcher;

    /** The partition discoverer, used to find new partitions. */
    private transient volatile AbstractPartitionDiscoverer partitionDiscoverer;

    /**
     * The offsets to restore to, if the consumer restores state from a checkpoint.
     *
     * <p>This map will be populated by the {@link #initializeState(FunctionInitializationContext)} method.
     *
     * <p>Using a sorted map as the ordering is important when using restored state
     * to seed the partition discoverer.
     */
    private transient volatile TreeMap<KafkaTopicPartition, String> restoredState;

    /** Accessor for state in the operator state backend. */
    private transient ListState<Tuple2<KafkaTopicPartition, String>> unionOffsetStates;

    /**
     * Flag indicating whether the consumer is restored from older state written with Flink 1.1 or 1.2.
     * When the current run is restored from older state, partition discovery is disabled.
     */
    private boolean restoredFromOldState;

    /** Discovery loop, executed in a separate thread. */
    private transient volatile Thread discoveryLoopThread;

    /** Flag indicating whether the consumer is still running. */
    private volatile boolean running = true;

    // ------------------------------------------------------------------------
    //  internal metrics
    // ------------------------------------------------------------------------

    /**
     * Flag indicating whether or not metrics should be exposed.
     * If {@code true}, offset metrics (e.g. current offset, committed offset) and
     * Kafka-shipped metrics will be registered.
     */
    private final boolean useMetrics;

    /** Counter for successful Kafka offset commits. */
    private transient Counter successfulCommits;

    /** Counter for failed Kafka offset commits. */
    private transient Counter failedCommits;

    /** Callback interface that will be invoked upon async Kafka commit completion.
     *  Please be aware that default callback implementation in base class does not
     *  provide any guarantees on thread-safety. This is sufficient for now because current
     *  supported Kafka connectors guarantee no more than 1 concurrent async pending offset
     *  commit.
     */
    private transient KafkaCommitCallback offsetCommitCallback;

    // ------------------------------------------------------------------------

    /** User-supplied properties for Kafka. **/
    protected final Properties properties;

    /** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
     * available. If 0, returns immediately with any records that are available now */
    protected final long pollTimeout;

    public FlinkDtsKafkaConsumer(
            String brokerUrl,
            String topic,
            String sid,
            String group,
            String user,
            String password,
            long startupOffsetsTimestamp,
            KafkaDeserializationSchema valueDeserializer) {
        this(brokerUrl, topic, sid, group, user, password, startupOffsetsTimestamp, valueDeserializer, null);
    }

    public FlinkDtsKafkaConsumer(
            String brokerUrl,
            String topic,
            String sid,
            String group,
            String user,
            String password,
            long startupOffsetsTimestamp,
            KafkaDeserializationSchema valueDeserializer,
            Properties kafkaExtraProps) {

        this.topicsDescriptor = new KafkaTopicsDescriptor(Collections.singletonList(topic), null);
        this.deserializer = valueDeserializer;

        Properties props = DtsKafkaUtil.getKafkaProperties(brokerUrl, topic, sid, group, user, password, kafkaExtraProps);

        this.properties = props;

        this.discoveryIntervalMillis = PropertiesUtil.getLong(Preconditions.checkNotNull(props, "props"),
                "flink.partition-discovery.interval-millis", Long.MIN_VALUE);

        this.useMetrics =  !PropertiesUtil.getBoolean(props, "flink.disable-metrics", false);

        if (startupOffsetsTimestamp > 0) {
            setStartFromTimestamp(startupOffsetsTimestamp);
        } else {
            setStartFromGroupOffsets();
        }

        // configure the polling timeout
        try {
            if (properties.containsKey(KEY_POLL_TIMEOUT)) {
                this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
            } else {
                this.pollTimeout = DEFAULT_POLL_TIMEOUT;
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
        }
    }

    /**
     * Specifies the consumer to start reading partitions from a specified timestamp.
     * The specified timestamp must be before the current timestamp.
     * This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
     *
     * <p>The consumer will look up the earliest offset whose timestamp is greater than or equal
     * to the specific timestamp from Kafka. If there's no such offset, the consumer will use the
     * latest offset to read data from kafka.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @param startupOffsetsTimestamp timestamp for the startup offsets, as milliseconds from epoch.
     *
     * @return The consumer object, to allow function chaining.
     */
    // NOTE -
    // This method is implemented in the base class because this is where the startup logging and verifications live.
    // However, it is not publicly exposed since only newer Kafka versions support the functionality.
    // Version-specific subclasses which can expose the functionality should override and allow public access.
    protected FlinkDtsKafkaConsumer<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
        checkArgument(startupOffsetsTimestamp >= 0, "The provided value for the startup offsets timestamp is invalid.");

        long currentTimestamp = System.currentTimeMillis();
        checkArgument(startupOffsetsTimestamp <= currentTimestamp,
                "Startup time[%s] must be before current time[%s].", startupOffsetsTimestamp, currentTimestamp);

        this.startupMode = StartupMode.TIMESTAMP;
        this.startupOffsetsTimestamp = startupOffsetsTimestamp;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading from any committed group offsets found
     * in Zookeeper / Kafka brokers. The "group.id" property must be set in the configuration
     * properties. If no offset can be found for a partition, the behaviour in "auto.offset.reset"
     * set in the configuration properties will be used for the partition.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkDtsKafkaConsumer<T> setStartFromGroupOffsets() {
        this.startupMode = StartupMode.GROUP_OFFSETS;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        // determine the offset commit mode
        this.offsetCommitMode =
                OffsetCommitModes.fromConfiguration(
                        getIsAutoCommitEnabled(),
                        enableCommitOnCheckpoints,
                        ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled());

        // create the partition discoverer
        this.partitionDiscoverer =
                createPartitionDiscoverer(
                        topicsDescriptor,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks());
        this.partitionDiscoverer.open();

        subscribedPartitionsToStartOffsets = new HashMap<>();
        final List<KafkaTopicPartition> allPartitions = partitionDiscoverer.discoverPartitions();
        if (restoredState != null) {
            for (KafkaTopicPartition partition : allPartitions) {
                if (!restoredState.containsKey(partition)) {
                    restoredState.put(partition, DtsUtil.composeCheckpint(
                            KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET,
                            INVALID_TIMESTAMP));
                }
            }

            for (Map.Entry<KafkaTopicPartition, String> restoredStateEntry :
                    restoredState.entrySet()) {
                // seed the partition discoverer with the union state while filtering out
                // restored partitions that should not be subscribed by this subtask
                if (KafkaTopicPartitionAssigner.assign(
                        restoredStateEntry.getKey(),
                        getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask()) {

                   long timestamp = DtsUtil.getTimestampFromCheckpint(restoredStateEntry.getValue());

                   //invalud timestamp, just use offset
                   if (timestamp == INVALID_TIMESTAMP) {
                       subscribedPartitionsToStartOffsets.put(restoredStateEntry.getKey(), DtsUtil.getOffsetFromCheckpint(restoredStateEntry.getValue()));
                   } else {
                       Map<KafkaTopicPartition, Long> remoteOffset = fetchOffsetsWithTimestamp(Collections.singleton(restoredStateEntry.getKey()), timestamp);
                       subscribedPartitionsToStartOffsets.put(restoredStateEntry.getKey(), remoteOffset.get(restoredStateEntry.getKey()));
                   }
                }
            }

            if (filterRestoredPartitionsWithCurrentTopicsDescriptor) {
                subscribedPartitionsToStartOffsets
                        .entrySet()
                        .removeIf(
                                entry -> {
                                    if (!topicsDescriptor.isMatchingTopic(
                                            entry.getKey().getTopic())) {
                                        LOG.warn(
                                                "{} is removed from subscribed partitions since it is no longer associated with topics descriptor of current execution.",
                                                entry.getKey());
                                        return true;
                                    }
                                    return false;
                                });
            }

            LOG.info(
                    "Consumer subtask {} will start reading {} partitions with offsets in restored state: {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    subscribedPartitionsToStartOffsets.size(),
                    subscribedPartitionsToStartOffsets);
        } else {
            // use the partition discoverer to fetch the initial seed partitions,
            // and set their initial offsets depending on the startup mode.
            // for SPECIFIC_OFFSETS and TIMESTAMP modes, we set the specific offsets now;
            // for other modes (EARLIEST, LATEST, and GROUP_OFFSETS), the offset is lazily
            // determined
            // when the partition is actually read.
            switch (startupMode) {
                case SPECIFIC_OFFSETS:
                    if (specificStartupOffsets == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to "
                                        + StartupMode.SPECIFIC_OFFSETS
                                        + ", but no specific offsets were specified.");
                    }

                    for (KafkaTopicPartition seedPartition : allPartitions) {
                        Long specificOffset = specificStartupOffsets.get(seedPartition);
                        if (specificOffset != null) {
                            // since the specified offsets represent the next record to read, we
                            // subtract
                            // it by one so that the initial state of the consumer will be correct
                            subscribedPartitionsToStartOffsets.put(
                                    seedPartition, specificOffset - 1);
                        } else {
                            // default to group offset behaviour if the user-provided specific
                            // offsets
                            // do not contain a value for this partition
                            subscribedPartitionsToStartOffsets.put(
                                    seedPartition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
                        }
                    }

                    break;
                case TIMESTAMP:
                    if (startupOffsetsTimestamp == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to "
                                        + StartupMode.TIMESTAMP
                                        + ", but no startup timestamp was specified.");
                    }

                    for (Map.Entry<KafkaTopicPartition, Long> partitionToOffset :
                            fetchOffsetsWithTimestamp(allPartitions, startupOffsetsTimestamp)
                                    .entrySet()) {
                        subscribedPartitionsToStartOffsets.put(
                                partitionToOffset.getKey(),
                                (partitionToOffset.getValue() == null)
                                        // if an offset cannot be retrieved for a partition with the
                                        // given timestamp,
                                        // we default to using the latest offset for the partition
                                        ? KafkaTopicPartitionStateSentinel.LATEST_OFFSET
                                        // since the specified offsets represent the next record to
                                        // read, we subtract
                                        // it by one so that the initial state of the consumer will
                                        // be correct
                                        : partitionToOffset.getValue() - 1);
                    }

                    break;
                default:
                    for (KafkaTopicPartition seedPartition : allPartitions) {
                        subscribedPartitionsToStartOffsets.put(
                                seedPartition, startupMode.getStateSentinel());
                    }
            }

            if (!subscribedPartitionsToStartOffsets.isEmpty()) {
                switch (startupMode) {
                    case EARLIEST:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the earliest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case LATEST:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the latest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case TIMESTAMP:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from timestamp {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                startupOffsetsTimestamp,
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case SPECIFIC_OFFSETS:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the specified startup offsets {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                specificStartupOffsets,
                                subscribedPartitionsToStartOffsets.keySet());

                        List<KafkaTopicPartition> partitionsDefaultedToGroupOffsets =
                                new ArrayList<>(subscribedPartitionsToStartOffsets.size());
                        for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition :
                                subscribedPartitionsToStartOffsets.entrySet()) {
                            if (subscribedPartition.getValue()
                                    == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
                                partitionsDefaultedToGroupOffsets.add(subscribedPartition.getKey());
                            }
                        }

                        if (partitionsDefaultedToGroupOffsets.size() > 0) {
                            LOG.warn(
                                    "Consumer subtask {} cannot find offsets for the following {} partitions in the specified startup offsets: {}"
                                            + "; their startup offsets will be defaulted to their committed group offsets in Kafka.",
                                    getRuntimeContext().getIndexOfThisSubtask(),
                                    partitionsDefaultedToGroupOffsets.size(),
                                    partitionsDefaultedToGroupOffsets);
                        }
                        break;
                    case GROUP_OFFSETS:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the committed group offsets in Kafka: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                }
            } else {
                LOG.info(
                        "Consumer subtask {} initially has no partitions to read from.",
                        getRuntimeContext().getIndexOfThisSubtask());
            }
        }

        this.deserializer.open(
                RuntimeContextInitializationContextAdapters.deserializationAdapter(
                        getRuntimeContext(), metricGroup -> metricGroup.addGroup("user")));
    }

    protected boolean getIsAutoCommitEnabled() {
        return getBoolean(properties, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true) &&
                PropertiesUtil.getLong(properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000) > 0;
    }

    protected AbstractPartitionDiscoverer createPartitionDiscoverer(
            KafkaTopicsDescriptor topicsDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks) {

        return new KafkaPartitionDiscoverer(
                topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties);
    }

    protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
            Collection<KafkaTopicPartition> partitions,
            long timestamp) {

        Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
        for (KafkaTopicPartition partition : partitions) {
            partitionOffsetsRequest.put(
                    new TopicPartition(partition.getTopic(), partition.getPartition()),
                    timestamp);
        }

        // use a short-lived consumer to fetch the offsets;
        // this is ok because this is a one-time operation that happens only on startup
        KafkaConsumer<?, ?> consumer = createKafkaConsumer(properties);

        Map<KafkaTopicPartition, Long> result = new HashMap<>(partitions.size());
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
                consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

            result.put(
                    new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
                    (partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
        }

        consumer.close();
        return result;
    }

    private KafkaConsumer createKafkaConsumer(Properties kafkaProperties) {
        try {
            return new KafkaConsumer<>(kafkaProperties);
        } catch (KafkaException e) {
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(null);
                return new KafkaConsumer<>(kafkaProperties);
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        if (subscribedPartitionsToStartOffsets == null) {
            throw new Exception("The partitions were not set for the consumer");
        }

        // initialize commit metrics and default offset callback method
        this.successfulCommits =
                this.getRuntimeContext()
                        .getMetricGroup()
                        .counter(COMMITS_SUCCEEDED_METRICS_COUNTER);
        this.failedCommits =
                this.getRuntimeContext().getMetricGroup().counter(COMMITS_FAILED_METRICS_COUNTER);
        final int subtaskIndex = this.getRuntimeContext().getIndexOfThisSubtask();

        this.offsetCommitCallback =
                new KafkaCommitCallback() {
                    @Override
                    public void onSuccess() {
                        successfulCommits.inc();
                    }

                    @Override
                    public void onException(Throwable cause) {
                        LOG.warn(
                                String.format(
                                        "Consumer subtask %d failed async Kafka commit.",
                                        subtaskIndex),
                                cause);
                        failedCommits.inc();
                    }
                };

        // mark the subtask as temporarily idle if there are no initial seed partitions;
        // once this subtask discovers some partitions and starts collecting records, the subtask's
        // status will automatically be triggered back to be active.
        if (subscribedPartitionsToStartOffsets.isEmpty()) {
            sourceContext.markAsTemporarilyIdle();
        }

        LOG.info(
                "Consumer subtask {} creating fetcher with offsets {}.",
                getRuntimeContext().getIndexOfThisSubtask(),
                subscribedPartitionsToStartOffsets);
        // from this point forward:
        //   - 'snapshotState' will draw offsets from the fetcher,
        //     instead of being built from `subscribedPartitionsToStartOffsets`
        //   - 'notifyCheckpointComplete' will start to do work (i.e. commit offsets to
        //     Kafka through the fetcher, if configured to do so)
        this.kafkaFetcher =
                createFetcher(
                        sourceContext,
                        subscribedPartitionsToStartOffsets,
                        watermarkStrategy,
                        (StreamingRuntimeContext) getRuntimeContext(),
                        offsetCommitMode,
                        getRuntimeContext().getMetricGroup().addGroup(KAFKA_CONSUMER_METRICS_GROUP),
                        useMetrics);

        if (!running) {
            return;
        }

        // depending on whether we were restored with the current state version (1.3),
        // remaining logic branches off into 2 paths:
        //  1) New state - partition discovery loop executed as separate thread, with this
        //                 thread running the main fetcher loop
        //  2) Old state - partition discovery is disabled and only the main fetcher loop is
        // executed
        if (discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED) {
            kafkaFetcher.runFetchLoop();
        } else {
            runWithPartitionDiscovery();
        }
    }

    private void runWithPartitionDiscovery() throws Exception {
        final AtomicReference<Exception> discoveryLoopErrorRef = new AtomicReference<>();
        createAndStartDiscoveryLoop(discoveryLoopErrorRef);

        kafkaFetcher.runFetchLoop();

        // make sure that the partition discoverer is waked up so that
        // the discoveryLoopThread exits
        partitionDiscoverer.wakeup();
        joinDiscoveryLoopThread();

        // rethrow any fetcher errors
        final Exception discoveryLoopError = discoveryLoopErrorRef.get();
        if (discoveryLoopError != null) {
            throw new RuntimeException(discoveryLoopError);
        }
    }

    @VisibleForTesting
    void joinDiscoveryLoopThread() throws InterruptedException {
        if (discoveryLoopThread != null) {
            discoveryLoopThread.join();
        }
    }

    private void createAndStartDiscoveryLoop(AtomicReference<Exception> discoveryLoopErrorRef) {
        discoveryLoopThread =
                new Thread(
                        () -> {
                            try {
                                // --------------------- partition discovery loop
                                // ---------------------

                                // throughout the loop, we always eagerly check if we are still
                                // running before
                                // performing the next operation, so that we can escape the loop as
                                // soon as possible

                                while (running) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "Consumer subtask {} is trying to discover new partitions ...",
                                                getRuntimeContext().getIndexOfThisSubtask());
                                    }

                                    final List<KafkaTopicPartition> discoveredPartitions;
                                    try {
                                        discoveredPartitions =
                                                partitionDiscoverer.discoverPartitions();
                                    } catch (AbstractPartitionDiscoverer.WakeupException
                                            | AbstractPartitionDiscoverer.ClosedException e) {
                                        // the partition discoverer may have been closed or woken up
                                        // before or during the discovery;
                                        // this would only happen if the consumer was canceled;
                                        // simply escape the loop
                                        break;
                                    }

                                    // no need to add the discovered partitions if we were closed
                                    // during the meantime
                                    if (running && !discoveredPartitions.isEmpty()) {
                                        kafkaFetcher.addDiscoveredPartitions(discoveredPartitions);
                                    }

                                    // do not waste any time sleeping if we're not running anymore
                                    if (running && discoveryIntervalMillis != 0) {
                                        try {
                                            Thread.sleep(discoveryIntervalMillis);
                                        } catch (InterruptedException iex) {
                                            // may be interrupted if the consumer was canceled
                                            // midway; simply escape the loop
                                            break;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                discoveryLoopErrorRef.set(e);
                            } finally {
                                // calling cancel will also let the fetcher loop escape
                                // (if not running, cancel() was already called)
                                if (running) {
                                    cancel();
                                }
                            }
                        },
                        "Kafka Partition Discovery for "
                                + getRuntimeContext().getTaskNameWithSubtasks());

        discoveryLoopThread.start();
    }

    protected DtsKafkaFetcher<T> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        return new DtsKafkaFetcher<>(
                sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarkStrategy,
                runtimeContext.getProcessingTimeService(),
                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                runtimeContext.getUserCodeClassLoader(),
                runtimeContext.getTaskNameWithSubtasks(),
                deserializer,
                properties,
                pollTimeout,
                runtimeContext.getMetricGroup(),
                consumerMetricGroup,
                useMetrics);
    }

    @Override
    public void cancel() {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        if (discoveryLoopThread != null) {

            if (partitionDiscoverer != null) {
                // we cannot close the discoverer here, as it is error-prone to concurrent access;
                // only wakeup the discoverer, the discovery loop will clean itself up after it escapes
                partitionDiscoverer.wakeup();
            }

            // the discovery loop may currently be sleeping in-between
            // consecutive discoveries; interrupt to shutdown faster
            discoveryLoopThread.interrupt();
        }

        // abort the fetcher, if there is one
        if (kafkaFetcher != null) {
            kafkaFetcher.cancel();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!running) {
            LOG.debug("notifyCheckpointComplete() called on closed source");
            return;
        }

        final DtsKafkaFetcher<?> fetcher = this.kafkaFetcher;
        if (fetcher == null) {
            LOG.debug("notifyCheckpointComplete() called on uninitialized source");
            return;
        }

        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
            // only one commit operation must be in progress
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing offsets to Kafka/ZooKeeper for checkpoint " + checkpointId);
            }

            try {
                final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
                if (posInMap == -1) {
                    LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
                    return;
                }

                @SuppressWarnings("unchecked")
                Map<KafkaTopicPartition, String> offsets =
                        (Map<KafkaTopicPartition, String>) pendingOffsetsToCommit.remove(posInMap);

                // remove older checkpoints in map
                for (int i = 0; i < posInMap; i++) {
                    pendingOffsetsToCommit.remove(0);
                }

                if (offsets == null || offsets.size() == 0) {
                    LOG.debug("Checkpoint state was empty.");
                    return;
                }

                Map<KafkaTopicPartition, Long> kafkaOffsets = new HashMap<>();

                for (Map.Entry<KafkaTopicPartition, String> entry : offsets.entrySet()) {
                    kafkaOffsets.put(entry.getKey(), DtsUtil.getOffsetFromCheckpint(entry.getValue()));
                }

                fetcher.commitInternalOffsetsToKafka(kafkaOffsets, offsetCommitCallback);
            } catch (Exception e) {
                if (running) {
                    throw e;
                }
                // else ignore exception if we are no longer running
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and restore
    // ------------------------------------------------------------------------

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        OperatorStateStore stateStore = context.getOperatorStateStore();

        this.unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                OFFSETS_STATE_NAME,
                                createStateSerializer(getRuntimeContext().getExecutionConfig())));

        if (context.isRestored()) {
            restoredState = new TreeMap<>(new KafkaTopicPartition.Comparator());

            // populate actual holder for restored state
            for (Tuple2<KafkaTopicPartition, String> kafkaOffset : unionOffsetStates.get()) {
                restoredState.put(kafkaOffset.f0, kafkaOffset.f1);
            }

            LOG.info(
                    "Consumer subtask {} restored state: {}.",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    restoredState);
        } else {
            LOG.info(
                    "Consumer subtask {} has no restore state.",
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    @VisibleForTesting
    static TupleSerializer<Tuple2<KafkaTopicPartition, String>> createStateSerializer(
            ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation and allow to
        // disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[] {
                        new KryoSerializer<>(KafkaTopicPartition.class, executionConfig),
                        StringSerializer.INSTANCE
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<KafkaTopicPartition, String>> tupleClass =
                (Class<Tuple2<KafkaTopicPartition, String>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

    @Override
    public final void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {
            unionOffsetStates.clear();

            final DtsKafkaFetcher<?> fetcher = this.kafkaFetcher;
            if (fetcher == null) {
                // the fetcher has not yet been initialized, which means we need to return the
                // originally restored offsets or the assigned partitions
                for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition :
                        subscribedPartitionsToStartOffsets.entrySet()) {
                    unionOffsetStates.add(Tuple2.of(subscribedPartition.getKey(), DtsUtil.composeCheckpint(subscribedPartition.getValue(), INVALID_TIMESTAMP)));
                }

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call
                    // can happen
                    // on this function at a time: either snapshotState() or
                    // notifyCheckpointComplete()
                    pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
                }
            } else {
                HashMap<KafkaTopicPartition, String> currentOffsets = fetcher.snapshotCurrentState();

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call
                    // can happen
                    // on this function at a time: either snapshotState() or
                    // notifyCheckpointComplete()
                    pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
                }

                for (Map.Entry<KafkaTopicPartition, String> kafkaTopicPartitionLongEntry :
                        currentOffsets.entrySet()) {
                    unionOffsetStates.add(
                            Tuple2.of(
                                    kafkaTopicPartitionLongEntry.getKey(),
                                    kafkaTopicPartitionLongEntry.getValue()));
                }
            }

            if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                // truncate the map of pending offsets to commit, to prevent infinite growth
                while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
                    pendingOffsetsToCommit.remove(0);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Configuration
    // ------------------------------------------------------------------------

    /**
     * Sets the given {@link WatermarkStrategy} on this consumer. These will be used to assign
     * timestamps to records and generates watermarks to signal event time progress.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source
     * (which you can do by using this method), per Kafka partition, allows users to let them
     * exploit the per-partition characteristics.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more than one partition.
     *
     * <p>Common watermark generation patterns can be found as static methods in the {@link
     * org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkDtsKafkaConsumer<T> assignTimestampsAndWatermarks(
            WatermarkStrategy<T> watermarkStrategy) {
        checkNotNull(watermarkStrategy);

        try {
            ClosureCleaner.clean(
                    watermarkStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.watermarkStrategy = new SerializedValue<>(watermarkStrategy);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "The given WatermarkStrategy is not serializable", e);
        }

        return this;
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated
     * manner. The watermark extractor will run per Kafka partition, watermarks will be merged
     * across partitions in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more than one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per
     * Kafka partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an {@link
     * AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     */
    @Deprecated
    public FlinkDtsKafkaConsumer<T> assignTimestampsAndWatermarks(
            AssignerWithPunctuatedWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms =
                    new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated
     * manner. The watermark extractor will run per Kafka partition, watermarks will be merged
     * across partitions in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more that one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per
     * Kafka partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an {@link
     * AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     */
    @Deprecated
    public FlinkDtsKafkaConsumer<T> assignTimestampsAndWatermarks(
            AssignerWithPeriodicWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms =
                    new AssignerWithPeriodicWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies whether or not the consumer should commit offsets back to Kafka on checkpoints.
     *
     * <p>This setting will only have effect if checkpointing is enabled for the job.
     * If checkpointing isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
     * property settings will be
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkDtsKafkaConsumer<T> setCommitOffsetsOnCheckpoints(boolean commitOnCheckpoints) {
        this.enableCommitOnCheckpoints = commitOnCheckpoints;
        return this;
    }
}
