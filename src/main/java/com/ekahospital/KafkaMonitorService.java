package com.ekahospital;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class KafkaMonitorService {

    private final AdminClient adminClient;

    public KafkaMonitorService() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        this.adminClient = AdminClient.create(properties);
    }

    private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
            throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, Long> groupOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
        }
        return groupOffset;
    }

    public void checkConsumerLag() {
        Log.info("Check lag");
        try {
            for (ConsumerGroupListing groupListing : adminClient.listConsumerGroups().all().get()) {
                String groupId = groupListing.groupId();
                Log.info("Group: "+ groupId);
                ConsumerGroupDescription groupDescription = adminClient.describeConsumerGroups(Collections.singletonList(groupId)).all().get().get(groupId);

                //Optional<TopicPartition> tp = groupDescription.members().stream().map(s -> s.assignment().topicPartitions());
                for (MemberDescription member : groupDescription.members()){
                    for (TopicPartition partition : member.assignment().topicPartitions()){
                        OffsetAndMetadata committed = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().get(partition);
                        long endOffset = adminClient.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest())).all().get().get(partition).offset();
                        long lag = endOffset - committed.offset();

                        Log.info("Topic " + lag);

                        if (lag > 100) {
                            sendAlert(groupId, partition, lag);
                        }

                    }
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void sendAlert(String groupId, TopicPartition partition, long lag) {
        // Implement your Telegram alert logic here
        System.out.println("ALERT " + partition.topic() + " " + lag);
    }
}
