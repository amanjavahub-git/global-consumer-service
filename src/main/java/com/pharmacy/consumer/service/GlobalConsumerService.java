package com.pharmacy.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.Queue;

@Service
public class GlobalConsumerService {

    // Store messages per topic
    private final Map<String, Queue<String>> topicMessages = new ConcurrentHashMap<>();

    // Track offset info for debugging
    private final Map<String, String> offsetInfo = new ConcurrentHashMap<>();

    /**
     * ✅ CRITICAL CONSUMER METHOD
     * - Listens to all configured topics (pharmacy-orders, lab-orders, billing-orders)
     * - Uses Manual Acknowledgment (AckMode.MANUAL) for explicit offset commits
     * - Only commits offset AFTER message is successfully processed
     * - If API is called before message is processed, it won't be returned
     */
    @KafkaListener(
        topics = "${app.kafka.consumer-topics}",
        groupId = "global-consumer",
        concurrency = "3"
    )
    public void consume(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.RECEIVED_KEY) String key,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment acknowledgment
    ) {
        try {
            // Format the message with metadata
            String formattedMsg = String.format(
                "Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s",
                topic, partition, offset, key, message
            );

            // Store in queue
            topicMessages.putIfAbsent(topic, new ConcurrentLinkedQueue<>());
            topicMessages.get(topic).add(formattedMsg);

            // Track offset info
            offsetInfo.put(topic + "-p" + partition, "Offset: " + offset);

            System.out.println("✅ Consumed: " + formattedMsg);

            // ✅ CRITICAL: Manually commit offset AFTER successful processing
            // This tells Kafka: "I have successfully processed this message"
            // Next time the consumer restarts, it will start from offset+1
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                System.out.println("✅ Offset committed for topic: " + topic +
                    ", partition: " + partition + ", offset: " + offset);
            }
        } catch (Exception e) {
            System.err.println("❌ Error consuming message: " + e.getMessage());
            e.printStackTrace();
            // ✅ NOTE: If NOT acknowledged, offset won't commit and message will be reprocessed
        }
    }

    /**
     * ✅ Get all messages for a topic (Does NOT clear the queue)
     * These are messages already consumed and offset is committed
     */
    public Queue<String> getMessagesByTopic(String topic) {
        return topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>());
    }

    /**
     * ✅ Get last message for a topic (Does NOT clear the queue)
     */
    public String getLastMessageByTopic(String topic) {
        Queue<String> msgs = topicMessages.get(topic);
        return (msgs == null || msgs.isEmpty()) ? "No messages consumed yet for topic: " + topic : msgs.peek();
    }

    /**
     * ✅ Get all messages and CLEAR the queue
     * Use this if you want to retrieve messages only once
     */
    public Queue<String> getAndClearMessagesByTopic(String topic) {
        Queue<String> msgs = topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>());
        topicMessages.put(topic, new ConcurrentLinkedQueue<>()); // Clear the queue
        return msgs;
    }

    /**
     * ✅ Get offset info for a specific partition
     */
    public String getOffsetInfo(String topic, int partition) {
        return offsetInfo.getOrDefault(topic + "-p" + partition, "No offset info available");
    }

    /**
     * ✅ Get all tracked offset information
     */
    public Map<String, String> getAllOffsetInfo() {
        return new ConcurrentHashMap<>(offsetInfo);
    }

    /**
     * ✅ Clear all in-memory messages and offset info
     * Useful for testing or resetting the consumer state
     */
    public void clearAllMessages() {
        topicMessages.clear();
        offsetInfo.clear();
        System.out.println("✅ All in-memory messages cleared");
    }

    /**
     * ✅ Get message count for a topic
     */
    public int getMessageCountByTopic(String topic) {
        return topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>()).size();
    }
}
