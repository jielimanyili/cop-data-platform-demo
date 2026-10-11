package com.jieli.cop.bench;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Reads pipeline progress via the admin client:
 * <ul>
 *   <li>{@link #topicEndOffset} — total messages produced to a topic (end offsets).</li>
 *   <li>{@link #lag} — consumer-group backlog on the input topic (end − committed),
 *       i.e. how far behind the Kafka Streams app is.</li>
 * </ul>
 * Comparing the output-topic growth rate against the producer rate tells you the
 * platform's sustained events/sec; a growing lag means the pipeline can't keep up.
 */
public final class PipelineMonitor implements AutoCloseable {

    private final AdminClient admin;
    private final String inputTopic;
    private final String groupId;

    public PipelineMonitor(String bootstrapServers, String inputTopic, String groupId) {
        this.inputTopic = inputTopic;
        this.groupId = groupId;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
    }

    /** Sum of end offsets across all partitions of {@code topic}. */
    public long topicEndOffset(String topic) throws Exception {
        return endOffsets(topic).values().stream().mapToLong(Long::longValue).sum();
    }

    /** Consumer-group lag (backlog) on the input topic. */
    public long lag() throws Exception {
        Map<TopicPartition, Long> ends = endOffsets(inputTopic);
        Map<TopicPartition, OffsetAndMetadata> current =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

        long lag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : current.entrySet()) {
            if (e.getKey().topic().equals(inputTopic)) {
                long end = ends.getOrDefault(e.getKey(), 0L);
                lag += Math.max(0, end - e.getValue().offset());
            }
        }
        return lag;
    }

    private Map<TopicPartition, Long> endOffsets(String topic) throws Exception {
        List<TopicPartition> tps = partitions(topic);
        Map<TopicPartition, OffsetSpec> request =
                tps.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        return admin.listOffsets(request).all().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private List<TopicPartition> partitions(String topic) throws Exception {
        TopicDescription desc =
                admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
        return desc.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
    }

    @Override
    public void close() {
        admin.close();
    }
}
