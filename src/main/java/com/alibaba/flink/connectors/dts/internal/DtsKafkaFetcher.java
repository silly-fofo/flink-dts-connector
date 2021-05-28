package com.alibaba.flink.connectors.dts.internal;

import com.alibaba.flink.connectors.dts.util.DtsUtil;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;

import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.COMMITTED_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.CURRENT_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.LEGACY_COMMITTED_OFFSETS_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.LEGACY_CURRENT_OFFSETS_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.OFFSETS_BY_PARTITION_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.OFFSETS_BY_TOPIC_METRICS_GROUP;

@Internal
public class DtsKafkaFetcher <T> {
    private static final Logger LOG = LoggerFactory.getLogger(DtsKafkaFetcher.class);

    private static final int NO_TIMESTAMPS_WATERMARKS = 0;
    private static final int WITH_WATERMARK_GENERATOR = 1;

    // ------------------------------------------------------------------------

    /** The source context to emit records and watermarks to. */
    protected final SourceFunction.SourceContext<T> sourceContext;

    /**
     * Wrapper around our SourceContext for allowing the {@link
     * org.apache.flink.api.common.eventtime.WatermarkGenerator} to emit watermarks and mark
     * idleness.
     */
    protected final WatermarkOutput watermarkOutput;

    /** {@link WatermarkOutputMultiplexer} for supporting per-partition watermark generation. */
    private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

    private final KafkaDeserializationSchema<T> deserializer;

    /** A collector to emit records in batch (bundle). * */
    private final KafkaCollector kafkaCollector;

    private final Handover handover;
    private final DtsKafkaConsumerThread consumerThread;
    private volatile boolean running = true;

    protected final ClosableBlockingQueue<DtsKafkaTopicPartitionState<T, TopicPartition>> unassignedPartitionsQueue;

    /** All partitions (and their state) that this fetcher is subscribed to. */
    private final List<DtsKafkaTopicPartitionState<T, TopicPartition>> subscribedPartitionStates;

    private final int timestampWatermarkMode;

    /**
     * Optional watermark strategy that will be run per Kafka partition, to exploit per-partition
     * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
     * it into multiple copies.
     */
    private final SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

    /** User class loader used to deserialize watermark assigners. */
    private final ClassLoader userCodeClassLoader;

    private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

    private final Object checkpointLock;

    // ------------------------------------------------------------------------
    //  Metrics
    // ------------------------------------------------------------------------

    /**
     * Flag indicating whether or not metrics should be exposed. If {@code true}, offset metrics
     * (e.g. current offset, committed offset) and Kafka-shipped metrics will be registered.
     */
    private final boolean useMetrics;

    /**
     * The metric group which all metrics for the consumer should be registered to. This metric
     * group is defined under the user scope
     */
    private final MetricGroup consumerMetricGroup;

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private final MetricGroup legacyCurrentOffsetsMetricGroup;

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private final MetricGroup legacyCommittedOffsetsMetricGroup;

    public DtsKafkaFetcher(SourceFunction.SourceContext<T> sourceContext,
                           Map<KafkaTopicPartition, Long> seedPartitionsWithInitialOffsets,
                           SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                           ProcessingTimeService processingTimeProvider,
                           long autoWatermarkInterval,
                           ClassLoader userCodeClassLoader,
                           String taskNameWithSubtasks,
                           KafkaDeserializationSchema<T> deserializer,
                           Properties kafkaProperties,
                           long pollTimeout,
                           MetricGroup subtaskMetricGroup,
                           MetricGroup consumerMetricGroup,
                           boolean useMetrics) throws Exception {
        this.sourceContext = (SourceFunction.SourceContext)Preconditions.checkNotNull(sourceContext);
        this.watermarkOutput = new SourceContextWatermarkOutputAdapter<>(sourceContext);
        this.watermarkOutputMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.userCodeClassLoader = checkNotNull(userCodeClassLoader);


        this.useMetrics = useMetrics;
        this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
        this.legacyCurrentOffsetsMetricGroup =
                consumerMetricGroup.addGroup(LEGACY_CURRENT_OFFSETS_METRICS_GROUP);
        this.legacyCommittedOffsetsMetricGroup =
                consumerMetricGroup.addGroup(LEGACY_COMMITTED_OFFSETS_METRICS_GROUP);

        this.watermarkStrategy = watermarkStrategy;

        if (watermarkStrategy == null) {
            timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
        } else {
            timestampWatermarkMode = WITH_WATERMARK_GENERATOR;
        }

        this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

        // initialize subscribed partition states with seed partitions
        this.subscribedPartitionStates =
                createPartitionStateHolders(
                        seedPartitionsWithInitialOffsets,
                        timestampWatermarkMode,
                        watermarkStrategy,
                        userCodeClassLoader);

        // check that all seed partition states have a defined offset
        for (DtsKafkaTopicPartitionState partitionState : subscribedPartitionStates) {
            if (!partitionState.isOffsetDefined()) {
                throw new IllegalArgumentException("The fetcher was assigned seed partitions with undefined initial offsets.");
            }
        }

        // all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
        for (DtsKafkaTopicPartitionState<T, TopicPartition> partition : subscribedPartitionStates) {
            unassignedPartitionsQueue.add(partition);
        }

        // register metrics for the initial seed partitions
        if (useMetrics) {
            registerOffsetMetrics(consumerMetricGroup, subscribedPartitionStates);
        }

        this.deserializer = deserializer;
        this.handover = new Handover();
        this.consumerThread = new DtsKafkaConsumerThread(
                LOG,
                this.handover,
                kafkaProperties,
                this.unassignedPartitionsQueue,
                this.getFetcherName() + " for " + taskNameWithSubtasks,
                pollTimeout,
                useMetrics,
                consumerMetricGroup,
                subtaskMetricGroup);

        this.kafkaCollector = new KafkaCollector();
    }

    /**
     * Adds a list of newly discovered partitions to the fetcher for consuming.
     *
     * <p>This method creates the partition state holder for each new partition, using {@link
     * KafkaTopicPartitionStateSentinel#EARLIEST_OFFSET} as the starting offset. It uses the
     * earliest offset because there may be delay in discovering a partition after it was created
     * and started receiving records.
     *
     * <p>After the state representation for a partition is created, it is added to the unassigned
     * partitions queue to await to be consumed.
     *
     * @param newPartitions discovered partitions to add
     */
    public void addDiscoveredPartitions(List<KafkaTopicPartition> newPartitions)
            throws IOException, ClassNotFoundException {
            List<DtsKafkaTopicPartitionState<T, TopicPartition>> newPartitionStates =
                    createPartitionStateHolders(
                            newPartitions,
                            KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET,
                            timestampWatermarkMode,
                            watermarkStrategy,
                            userCodeClassLoader);

        if (useMetrics) {
            registerOffsetMetrics(consumerMetricGroup, newPartitionStates);
        }

        for (DtsKafkaTopicPartitionState<T, TopicPartition> newPartitionState : newPartitionStates) {
            // the ordering is crucial here; first register the state holder, then
            // push it to the partitions queue to be read
            subscribedPartitionStates.add(newPartitionState);
            unassignedPartitionsQueue.add(newPartitionState);
        }
    }

    // ------------------------------------------------------------------------
    //  Fetcher work methods
    // ------------------------------------------------------------------------

    public void runFetchLoop() throws Exception {
        try {
            // kick off the actual Kafka consumer
            consumerThread.start();

            while (running) {
                // this blocks until we get the next records
                // it automatically re-throws exceptions encountered in the consumer thread
                final ConsumerRecords<byte[], byte[]> records = handover.pollNext();

                // get the records for each topic partition
                for (DtsKafkaTopicPartitionState<T, TopicPartition> partition :
                        subscribedPartitionStates()) {

                    List<ConsumerRecord<byte[], byte[]>> partitionRecords =
                            records.records(partition.getKafkaPartitionHandle());

                    partitionConsumerRecordsHandler(partitionRecords, partition);
                }
            }
        } finally {
            // this signals the consumer thread that no more work is to be done
            consumerThread.shutdown();
        }

        // on a clean exit, wait for the runner thread
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            // may be the result of a wake-up interruption after an exception.
            // we ignore this here and only restore the interruption state
            Thread.currentThread().interrupt();
        }
    }

    public void cancel() {
        this.running = false;
        this.handover.close();
        this.consumerThread.shutdown();
    }

    /** Gets the name of this fetcher, for thread naming and logging purposes. */
    protected String getFetcherName() {
        return "Dts Kafka Fetcher";
    }

    protected void partitionConsumerRecordsHandler(
            List<ConsumerRecord<byte[], byte[]>> partitionRecords,
            DtsKafkaTopicPartitionState<T, TopicPartition> partition)
            throws Exception {

        for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
            deserializer.deserialize(record, kafkaCollector);

            // emit the actual records. this also updates offset state atomically and emits
            // watermarks
            emitRecordsWithTimestamps(
                    kafkaCollector.getRecords(), partition, record.offset(), record.timestamp());

            if (kafkaCollector.isEndOfStreamSignalled()) {
                // end of stream signaled
                running = false;
                break;
            }
        }
    }

    // ------------------------------------------------------------------------
    //  emitting records
    // ------------------------------------------------------------------------

    /**
     * Emits a record attaching a timestamp to it.
     *
     * @param records The records to emit
     * @param partitionState The state of the Kafka partition from which the record was fetched
     * @param offset The offset of the corresponding Kafka record
     * @param kafkaEventTimestamp The timestamp of the Kafka record
     */
    protected void emitRecordsWithTimestamps(
            Queue<T> records,
            DtsKafkaTopicPartitionState<T, TopicPartition> partitionState,
            long offset,
            long kafkaEventTimestamp) {
        // emit the records, using the checkpoint lock to guarantee
        // atomicity of record emission and offset state update
        synchronized (checkpointLock) {
            T record;
            while ((record = records.poll()) != null) {
                long timestamp = partitionState.extractTimestamp(record, kafkaEventTimestamp);
                sourceContext.collectWithTimestamp(record, timestamp);

                // this might emit a watermark, so do it after emitting the record
                partitionState.onEvent(record, timestamp);
            }
            partitionState.setOffset(offset);
            partitionState.setTimestamp(DtsUtil.getTimestampSeconds(kafkaEventTimestamp));
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Shortcut variant of {@link #createPartitionStateHolders(Map, int, SerializedValue,
     * ClassLoader)} that uses the same offset for all partitions when creating their state holders.
     */
    private List<DtsKafkaTopicPartitionState<T, TopicPartition>> createPartitionStateHolders(
            List<KafkaTopicPartition> partitions,
            long initialOffset,
            int timestampWatermarkMode,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ClassLoader userCodeClassLoader)
            throws IOException, ClassNotFoundException {

        Map<KafkaTopicPartition, Long> partitionsToInitialOffset = new HashMap<>(partitions.size());
        for (KafkaTopicPartition partition : partitions) {
            partitionsToInitialOffset.put(partition, initialOffset);
        }

        return createPartitionStateHolders(
                partitionsToInitialOffset,
                timestampWatermarkMode,
                watermarkStrategy,
                userCodeClassLoader);
    }

    /**
     * Utility method that takes the topic partitions and creates the topic partition state holders,
     * depending on the timestamp / watermark mode.
     */
    private List<DtsKafkaTopicPartitionState<T, TopicPartition>> createPartitionStateHolders(
            Map<KafkaTopicPartition, Long> partitionsToInitialOffsets,
            int timestampWatermarkMode,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ClassLoader userCodeClassLoader)
            throws IOException, ClassNotFoundException {

        // CopyOnWrite as adding discovered partitions could happen in parallel
        // while different threads iterate the partitions list
        List<DtsKafkaTopicPartitionState<T, TopicPartition>> partitionStates = new CopyOnWriteArrayList<>();

        switch (timestampWatermarkMode) {
            case NO_TIMESTAMPS_WATERMARKS:
            {
                for (Map.Entry<KafkaTopicPartition, Long> partitionEntry :
                        partitionsToInitialOffsets.entrySet()) {
                    // create the kafka version specific partition handle
                    TopicPartition kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());

                    DtsKafkaTopicPartitionState<T, TopicPartition> partitionState =
                            new DtsKafkaTopicPartitionState<>(
                                    partitionEntry.getKey(), kafkaHandle);
                    partitionState.setOffset(partitionEntry.getValue());

                    partitionStates.add(partitionState);
                }

                return partitionStates;
            }

            case WITH_WATERMARK_GENERATOR:
            {
                for (Map.Entry<KafkaTopicPartition, Long> partitionEntry :
                        partitionsToInitialOffsets.entrySet()) {
                    final KafkaTopicPartition kafkaTopicPartition = partitionEntry.getKey();
                    TopicPartition kafkaHandle = createKafkaPartitionHandle(kafkaTopicPartition);
                    WatermarkStrategy<T> deserializedWatermarkStrategy =
                            watermarkStrategy.deserializeValue(userCodeClassLoader);

                    // the format of the ID does not matter, as long as it is unique
                    final String partitionId =
                            kafkaTopicPartition.getTopic()
                                    + '-'
                                    + kafkaTopicPartition.getPartition();
                    watermarkOutputMultiplexer.registerNewOutput(partitionId);
                    WatermarkOutput immediateOutput =
                            watermarkOutputMultiplexer.getImmediateOutput(partitionId);
                    WatermarkOutput deferredOutput =
                            watermarkOutputMultiplexer.getDeferredOutput(partitionId);

                    DtsKafkaTopicPartitionStateWithWatermarkGenerator<T, TopicPartition> partitionState =
                            new DtsKafkaTopicPartitionStateWithWatermarkGenerator<>(
                                    partitionEntry.getKey(),
                                    kafkaHandle,
                                    deserializedWatermarkStrategy.createTimestampAssigner(
                                            () -> consumerMetricGroup),
                                    deserializedWatermarkStrategy.createWatermarkGenerator(
                                            () -> consumerMetricGroup),
                                    immediateOutput,
                                    deferredOutput);

                    partitionState.setOffset(partitionEntry.getValue());

                    partitionStates.add(partitionState);
                }

                return partitionStates;
            }

            default:
                // cannot happen, add this as a guard for the future
                throw new RuntimeException();
        }
    }

    // ------------------------- Metrics ----------------------------------

    /**
     * For each partition, register a new metric group to expose current offsets and committed
     * offsets. Per-partition metric groups can be scoped by user variables {@link
     * KafkaConsumerMetricConstants#OFFSETS_BY_TOPIC_METRICS_GROUP} and {@link
     * KafkaConsumerMetricConstants#OFFSETS_BY_PARTITION_METRICS_GROUP}.
     *
     * <p>Note: this method also registers gauges for deprecated offset metrics, to maintain
     * backwards compatibility.
     *
     * @param consumerMetricGroup The consumer metric group
     * @param partitionOffsetStates The partition offset state holders, whose values will be used to
     *     update metrics
     */
    private void registerOffsetMetrics(
            MetricGroup consumerMetricGroup,
            List<DtsKafkaTopicPartitionState<T, TopicPartition>> partitionOffsetStates) {

        for (KafkaTopicPartitionState<T, TopicPartition> ktp : partitionOffsetStates) {
            MetricGroup topicPartitionGroup =
                    consumerMetricGroup
                            .addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, ktp.getTopic())
                            .addGroup(
                                    OFFSETS_BY_PARTITION_METRICS_GROUP,
                                    Integer.toString(ktp.getPartition()));

            topicPartitionGroup.gauge(
                    CURRENT_OFFSETS_METRICS_GAUGE,
                    new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
            topicPartitionGroup.gauge(
                    COMMITTED_OFFSETS_METRICS_GAUGE,
                    new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));

            legacyCurrentOffsetsMetricGroup.gauge(
                    getLegacyOffsetsMetricsGaugeName(ktp),
                    new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
            legacyCommittedOffsetsMetricGroup.gauge(
                    getLegacyOffsetsMetricsGaugeName(ktp),
                    new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));
        }
    }

    private static String getLegacyOffsetsMetricsGaugeName(KafkaTopicPartitionState<?, ?> ktp) {
        return ktp.getTopic() + "-" + ktp.getPartition();
    }

    protected final List<DtsKafkaTopicPartitionState<T, TopicPartition>> subscribedPartitionStates() {
        return this.subscribedPartitionStates;
    }

    public TopicPartition createKafkaPartitionHandle(KafkaTopicPartition partition) {
        return new TopicPartition(partition.getTopic(), partition.getPartition());
    }

    /**
     * Commits the given partition offsets to the Kafka brokers (or to ZooKeeper for
     * older Kafka versions). This method is only ever called when the offset commit mode of
     * the consumer is {@link OffsetCommitMode#ON_CHECKPOINTS}.
     *
     * <p>The given offsets are the internal checkpointed offsets, representing
     * the last processed record of each partition. Version-specific implementations of this method
     * need to hold the contract that the given offsets must be incremented by 1 before
     * committing them, so that committed offsets to Kafka represent "the next record to process".
     *
     * @param offsets The offsets to commit to Kafka (implementations must increment offsets by 1 before committing).
     * @param commitCallback The callback that the user should trigger when a commit request completes or fails.
     * @throws Exception This method forwards exceptions.
     */
    /**
     * Commits the given partition offsets to the Kafka brokers (or to ZooKeeper for older Kafka
     * versions). This method is only ever called when the offset commit mode of the consumer is
     * {@link OffsetCommitMode#ON_CHECKPOINTS}.
     *
     * <p>The given offsets are the internal checkpointed offsets, representing the last processed
     * record of each partition. Version-specific implementations of this method need to hold the
     * contract that the given offsets must be incremented by 1 before committing them, so that
     * committed offsets to Kafka represent "the next record to process".
     *
     * @param offsets The offsets to commit to Kafka (implementations must increment offsets by 1
     *     before committing).
     * @param commitCallback The callback that the user should trigger when a commit request
     *     completes or fails.
     * @throws Exception This method forwards exceptions.
     */
    public final void commitInternalOffsetsToKafka(
            Map<KafkaTopicPartition, Long> offsets, @Nonnull KafkaCommitCallback commitCallback)
            throws Exception {
        // Ignore sentinels. They might appear here if snapshot has started before actual offsets
        // values
        // replaced sentinels
        doCommitInternalOffsetsToKafka(filterOutSentinels(offsets), commitCallback);
    }

    protected void doCommitInternalOffsetsToKafka(
            Map<KafkaTopicPartition, Long> offsets, @Nonnull KafkaCommitCallback commitCallback)
            throws Exception {

        @SuppressWarnings("unchecked")
        List<DtsKafkaTopicPartitionState<T, TopicPartition>> partitions = subscribedPartitionStates();

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(partitions.size());

        for (KafkaTopicPartitionState<T, TopicPartition> partition : partitions) {
            Long lastProcessedOffset = offsets.get(partition.getKafkaTopicPartition());
            if (lastProcessedOffset != null) {
                checkState(lastProcessedOffset >= 0, "Illegal offset value to commit");

                // committed offsets through the KafkaConsumer need to be 1 more than the last
                // processed offset.
                // This does not affect Flink's checkpoints/saved state.
                long offsetToCommit = lastProcessedOffset + 1;

                offsetsToCommit.put(
                        partition.getKafkaPartitionHandle(), new OffsetAndMetadata(offsetToCommit));
                partition.setCommittedOffset(offsetToCommit);
            }
        }

        // record the work to be committed by the main consumer thread and make sure the consumer
        // notices that
        consumerThread.setOffsetsToCommit(offsetsToCommit, commitCallback);
    }

    private Map<KafkaTopicPartition, Long> filterOutSentinels(
            Map<KafkaTopicPartition, Long> offsets) {
        return offsets.entrySet().stream()
                .filter(entry -> !KafkaTopicPartitionStateSentinel.isSentinel(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // ------------------------------------------------------------------------
    //  snapshot and restore the state
    // ------------------------------------------------------------------------

    /**
     * Takes a snapshot of the partition offsets.
     *
     * <p>Important: This method must be called under the checkpoint lock.
     *
     * @return A map from partition to current offset.
     */
    public HashMap<KafkaTopicPartition, String> snapshotCurrentState() {
        assert Thread.holdsLock(this.checkpointLock);

        HashMap<KafkaTopicPartition, String> state = new HashMap<>(subscribedPartitionStates.size());
        for (DtsKafkaTopicPartitionState<T, TopicPartition> partition : subscribedPartitionStates) {
            state.put(partition.getKafkaTopicPartition(), DtsUtil.composeCheckpint(partition.getOffset(), partition.getTimestamp()));
        }
        return state;
    }

    /**
     * Gauge types.
     */
    private enum OffsetGaugeType {
        CURRENT_OFFSET,
        COMMITTED_OFFSET
    }

    /** Gauge for getting the offset of a KafkaTopicPartitionState. */
    private static class OffsetGauge implements Gauge<Long> {

        private final KafkaTopicPartitionState<?, ?> ktp;
        private final OffsetGaugeType gaugeType;

        OffsetGauge(KafkaTopicPartitionState<?, ?> ktp, OffsetGaugeType gaugeType) {
            this.ktp = ktp;
            this.gaugeType = gaugeType;
        }

        @Override
        public Long getValue() {
            switch (gaugeType) {
                case COMMITTED_OFFSET:
                    return ktp.getCommittedOffset();
                case CURRENT_OFFSET:
                    return ktp.getOffset();
                default:
                    throw new RuntimeException("Unknown gauge type: " + gaugeType);
            }
        }
    }

    private class KafkaCollector implements Collector<T> {
        private final Queue<T> records = new ArrayDeque<>();

        private boolean endOfStreamSignalled = false;

        @Override
        public void collect(T record) {
            // do not emit subsequent elements if the end of the stream reached
            if (endOfStreamSignalled || deserializer.isEndOfStream(record)) {
                endOfStreamSignalled = true;
                return;
            }
            records.add(record);
        }

        public Queue<T> getRecords() {
            return records;
        }

        public boolean isEndOfStreamSignalled() {
            return endOfStreamSignalled;
        }

        @Override
        public void close() {}
    }
}

